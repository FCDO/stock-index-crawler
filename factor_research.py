# -*- coding: utf-8 -*-
"""
因子研究：在 2001-2019 開發樣本上篩選穩健因子，2020-2026 僅做一次性最終測試。

方法論（在看到任何結果前先承諾）：
- 候選因子全部來自有文獻/經濟邏輯的效應，每個最多 1-2 個參數
- 禁用條件平均查表（前一版過擬合的根源）
- 存活標準（寫死）：開發期淨 Sharpe >= 0.7（約 t>=3）、19 年中 >= 12 年為正、
  同家族至少 2 組參數 Sharpe >= 0.5（參數高原，防單點尖峰）
- 參數選擇：合格參數中取「中位」而非最佳（反挑櫻桃）
- 最多取 3 個家族，等權投票合成，2020-2026 只跑一次
- 全程含成本（每邊 1.5 點）、結算日開盤強制平倉（與正式版相同）

候選清單（共 7 家族、20 組設定，全部列出，不論結果好壞）：
  1. SMA_LF  趨勢/季線濾網（多/空手）   N in {20,60,120,200}   理由：時序動能文獻
  2. SMA_LS  趨勢（多/空）             N in {20,60,120,200}
  3. TSMOM   N日報酬符號（多/空）       N in {20,60,120}
  4. GAP     隔夜跳空順勢（當日沖）      θ in {0, 0.1%, 0.3%}   理由：隔夜資訊延續(美股領先)
  5. BASIS   期現價差偏離（多/空/空手）  θ in {0.2%, 0.4%}      理由：carry；探索性
  6. RS      櫃買-加權相對強弱(風險偏好) N in {20,60}           探索性
  7. VOTE4   現行票4（上影線+振幅）      固定參數               重新誠實評估
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5  # 每邊成本（點）：手續費+稅+滑價

# ================= 資料載入 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH RankedData AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) as rn
        FROM tx_futures WHERE session = '一般'
    )
    SELECT trading_date, open_price, high_price, low_price, close_price, volume, delivery_month
    FROM RankedData WHERE rn = 1 ORDER BY trading_date
""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tw = pd.read_sql_query("SELECT date, close AS tw_close FROM twse_index ORDER BY date", conn)
tp = pd.read_sql_query("SELECT date, close AS tp_close FROM tpex_index ORDER BY date", conn)
conn.close()

df['trading_date'] = pd.to_datetime(df['trading_date'])
tw['date'] = pd.to_datetime(tw['date'])
tp['date'] = pd.to_datetime(tp['date'])
df = df.merge(tw, left_on='trading_date', right_on='date', how='left').drop(columns='date')
df = df.merge(tp, left_on='trading_date', right_on='date', how='left').drop(columns='date')
df['tw_close'] = df['tw_close'].ffill()
df['tp_close'] = df['tp_close'].ffill()

n = len(df)
dates = df['trading_date']
yr = dates.dt.year.values
o = df['open_price'].values.astype(float)
h = df['high_price'].values.astype(float)
l = df['low_price'].values.astype(float)
c = df['close_price'].values.astype(float)
dm = df['delivery_month'].values

roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]          # 新合約第一天（跨合約，隔夜不可比）
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]          # 結算日（與正式版相同定義）

# 回補連續序列（訊號用）：跨合約日只取當日日內變化，去除換月基差跳動
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)

DEV = (yr >= 2001) & (yr <= 2019)
TEST = (yr >= 2020)
DEV_YEARS = 19


def shift1(x):
    out = np.empty_like(x, dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


# ================= 回測引擎 =================
def run_cont(sig, cost=COST):
    """連續部位引擎：sig[t]=當日應持部位(由 t-1 收盤前資訊決定)，t 日開盤執行。
    結算日開盤強平、不建新倉（與正式版一致）。含每邊成本。"""
    pnl = np.zeros(n)
    sides = 0
    pos = 0
    ep = np.nan
    for t in range(1, n):
        p = 0.0
        tc = 0.0
        if is_settle[t]:
            if pos == 1:
                p = o[t] - ep
            elif pos == -1:
                p = ep - o[t]
            if pos != 0:
                tc = cost
                sides += 1
            pos = 0
            ep = np.nan
        else:
            tgt = 0 if np.isnan(sig[t]) else int(sig[t])
            if tgt != pos:
                if pos == 1:
                    p += o[t] - ep
                elif pos == -1:
                    p += ep - o[t]
                tc += cost * abs(tgt - pos)
                sides += abs(tgt - pos)
                pos = tgt
                ep = o[t] if pos != 0 else np.nan
            if pos == 1:
                p += c[t] - ep
                ep = c[t]
            elif pos == -1:
                p += ep - c[t]
                ep = c[t]
        pnl[t] = p - tc
    return pnl, sides


def run_intra(sig, cost=COST):
    """當沖引擎：sig[t] 於 t 日開盤已知（用到 open[t]），開盤進、收盤出，不留倉。"""
    s = np.nan_to_num(sig, nan=0.0)
    pnl = s * (c - o) - 2 * cost * np.abs(s)
    pnl[0] = 0.0
    return pnl, int(2 * np.abs(s).sum())


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    tt = x.mean() / sd * np.sqrt(len(x)) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, t=tt, mdd=mdd, ys=ys,
                posy=int((ys > 0).sum()))


# ================= 候選因子 =================
candidates = []  # (family, name, param_order, engine, sig)

# 1/2. SMA 趨勢
for N in [20, 60, 120, 200]:
    sma = pd.Series(adjC).rolling(N).mean().values
    above = shift1((adjC > sma).astype(float))          # t-1 收盤狀態
    valid = ~np.isnan(shift1(sma))
    lf = np.where(valid, np.where(above == 1, 1, 0), 0)
    ls = np.where(valid, np.where(above == 1, 1, -1), 0)
    candidates.append(('SMA_LF', f'SMA_LF({N})', N, 'cont', lf))
    candidates.append(('SMA_LS', f'SMA_LS({N})', N, 'cont', ls))

# 3. TSMOM
for N in [20, 60, 120]:
    mom = shift1(adjC - np.concatenate([np.full(N, np.nan), adjC[:-N]]))
    sig = np.where(np.isnan(mom), 0, np.where(mom > 0, 1, -1))
    candidates.append(('TSMOM', f'TSMOM({N})', N, 'cont', sig))

# 4. GAP 隔夜跳空順勢（當沖）
gap = np.full(n, np.nan)
gap[1:] = (o[1:] - c[:-1]) / c[:-1]
gap[roll_first] = np.nan                                 # 跨合約隔夜不可比
for TH in [0.0, 0.001, 0.003]:
    sig = np.where(np.isnan(gap), 0, np.where(gap > TH, 1, np.where(gap < -TH, -1, 0)))
    candidates.append(('GAP', f'GAP({TH:.1%})', TH, 'intra', sig))

# 5. BASIS 期現價差偏離 60 日均值
basis = c / df['tw_close'].values - 1
bdev = basis - pd.Series(basis).rolling(60).mean().values
bd1 = shift1(bdev)
for TH in [0.002, 0.004]:
    sig = np.where(np.isnan(bd1), 0, np.where(bd1 < -TH, 1, np.where(bd1 > TH, -1, 0)))
    candidates.append(('BASIS', f'BASIS({TH:.1%})', TH, 'cont', sig))

# 6. RS 櫃買-加權相對強弱
for N in [20, 60]:
    rs = (df['tp_close'].pct_change(N) - df['tw_close'].pct_change(N)).values
    rs1 = shift1(rs)
    sig = np.where(np.isnan(rs1), 0, np.where(rs1 > 0, 1, -1))
    candidates.append(('RS', f'RS({N})', N, 'cont', sig))

# 7. VOTE4 現行票4
us = (h - np.maximum(o, c)) / o
ir = (h - l) / o
us_s = pd.Series(us).rolling(2).mean().values
us_l = pd.Series(us).rolling(10).mean().values
ir_s = pd.Series(ir).rolling(5).mean().values
ir_l = pd.Series(ir).rolling(10).mean().values
score = ((us_s > us_l).astype(int) + (ir_s > ir_l).astype(int)).astype(float)
score[np.isnan(us_l) | np.isnan(ir_l)] = np.nan
sc1 = shift1(score)
sig = np.where(np.isnan(sc1), 0, np.where(sc1 == 2, 1, np.where(sc1 == 0, -1, 0)))
candidates.append(('VOTE4', 'VOTE4', 0, 'cont', sig))

# ================= 基準 =================
pnl_long, sides_long = run_cont(np.ones(n))
base_dev = stats(pnl_long, DEV)
base_test = stats(pnl_long, TEST)

print("=" * 100)
print(f"開發樣本 2001-2019（{int(DEV.sum())} 天）   基準[永遠做多]: "
      f"淨損益 {base_dev['tot']:>8,.0f}  Sharpe {base_dev['sharpe']:.2f}  正年 {base_dev['posy']}/19")
print("=" * 100)
print(f"{'家族':<8} {'設定':<14} {'淨損益':>9} {'Sharpe':>7} {'t值':>6} {'正年':>5} {'MDD':>8} {'邊數':>6}")
print("-" * 100)

results = {}
for fam, name, prm, eng, sig in candidates:
    pnl, sides = (run_cont(sig) if eng == 'cont' else run_intra(sig))
    st = stats(pnl, DEV)
    results[name] = dict(family=fam, param=prm, engine=eng, sig=sig, pnl=pnl,
                         dev=st, sides=sides)
    print(f"{fam:<8} {name:<14} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['t']:>6.2f} "
          f"{st['posy']:>4}/19 {st['mdd']:>8,.0f} {sides:>6}")

# ================= 機械式存活判定（規則寫死） =================
print()
print("=" * 100)
print("存活判定：家族最佳 Sharpe>=0.7 且 該設定正年>=12/19 且 家族內 >=2 組 Sharpe>=0.5（單設定家族需自身達標）")
print("=" * 100)
families = {}
for name, r in results.items():
    families.setdefault(r['family'], []).append(name)

survivors = []
for fam, names in families.items():
    sh = {nm: results[nm]['dev']['sharpe'] for nm in names}
    best = max(sh, key=sh.get)
    plateau_ok = sum(1 for v in sh.values() if v >= 0.5) >= (2 if len(names) > 1 else 1)
    qual = (sh[best] >= 0.7 and results[best]['dev']['posy'] >= 12 and plateau_ok)
    if qual:
        oks = sorted([nm for nm in names if sh[nm] >= 0.5], key=lambda x: results[x]['param'])
        pick = oks[len(oks) // 2]  # 合格參數取中位（反挑櫻桃）
        survivors.append(pick)
        print(f"[PASS] {fam:<8} 最佳 {best}(Sh={sh[best]:.2f})  入選設定(中位參數)={pick}")
    else:
        print(f"[FAIL] {fam:<8} 最佳 {best}(Sh={sh[best]:.2f}, 正年{results[best]['dev']['posy']}/19, "
              f"高原{'O' if plateau_ok else 'X'})")

survivors = sorted(survivors, key=lambda x: -results[x]['dev']['sharpe'])[:3]
print(f"\n>>> 最終存活（最多3）: {survivors if survivors else '（無）'}")

# ================= 合成 + 一次性最終測試 =================
if survivors:
    cont_sv = [nm for nm in survivors if results[nm]['engine'] == 'cont']
    intra_sv = [nm for nm in survivors if results[nm]['engine'] == 'intra']

    pnl_comb = np.zeros(n)
    desc = []
    if cont_sv:
        vote_sum = np.sum([results[nm]['sig'] for nm in cont_sv], axis=0)
        comb_sig = np.sign(vote_sum)
        pnl_c, sides_c = run_cont(comb_sig)
        pnl_comb += pnl_c
        desc.append(f"連續艙=多數決({'+'.join(cont_sv)})")
    if intra_sv:
        for nm in intra_sv:
            pnl_i, _ = run_intra(results[nm]['sig'])
            pnl_comb += pnl_i
        desc.append(f"當沖艙={'+'.join(intra_sv)}")

    print()
    print("=" * 100)
    print(f"一次性最終測試 2020-2026/06   組合: {'; '.join(desc)}")
    print("=" * 100)
    print(f"{'策略':<26} {'淨損益':>9} {'Sharpe':>7} {'正年':>5} {'MDD':>8}")
    print("-" * 100)
    for nm in survivors:
        st = stats(results[nm]['pnl'], TEST)
        print(f"{nm:<26} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['posy']:>4}/7 {st['mdd']:>8,.0f}")
    st = stats(pnl_comb, TEST)
    std = stats(pnl_comb, DEV)
    print(f"{'組合(等權)':<26} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['posy']:>4}/7 {st['mdd']:>8,.0f}")
    print(f"{'基準: 永遠做多':<26} {base_test['tot']:>9,.0f} {base_test['sharpe']:>7.2f} "
          f"{base_test['posy']:>4}/7 {base_test['mdd']:>8,.0f}")
    print(f"\n組合開發期(參考): 淨損益 {std['tot']:,.0f}  Sharpe {std['sharpe']:.2f}  正年 {std['posy']}/19  MDD {std['mdd']:,.0f}")

    print(f"\n組合年度明細（測試期）:")
    yl = stats(pnl_long, TEST)['ys']
    for y, v in st['ys'].items():
        print(f"  {y}: 組合 {v:>+8,.0f}   永遠做多 {yl.get(y, 0):>+8,.0f}")

    # 成本敏感度（僅組合）
    print(f"\n成本敏感度（每邊 3 點重算組合）:")
    pnl_comb3 = np.zeros(n)
    if cont_sv:
        pnl_c3, _ = run_cont(comb_sig, cost=3.0)
        pnl_comb3 += pnl_c3
    for nm in intra_sv:
        pnl_i3, _ = run_intra(results[nm]['sig'], cost=3.0)
        pnl_comb3 += pnl_i3
    st3d, st3t = stats(pnl_comb3, DEV), stats(pnl_comb3, TEST)
    print(f"  開發期: {st3d['tot']:,.0f} (Sh {st3d['sharpe']:.2f})   測試期: {st3t['tot']:,.0f} (Sh {st3t['sharpe']:.2f})")

print("\n[done] 所有 20 組設定之開發期結果已完整列出（含未存活者），無事後挑選。")
