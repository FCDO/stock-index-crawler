# -*- coding: utf-8 -*-
"""
risk_layer_research.py - 部位/風險層研究（非因子挖掘）

定位聲明：第6輪封槍承諾為「不再追加日線『因子』輪次」——本研究不產生任何新的
方向預測，只研究既有已部署訊號（四票 PIT / SMA200 趨勢 / LT 紙上）的【部位大小】
與【執行會計】，屬封槍範圍之外的營運層。仍依本專案慣例：規則先寫死、dev 決策、
2020-2026 僅描述性（該窗已多次揭露，對在場策略不再具確認效力）。

═══ 事前承諾（運行前寫定，禁止事後調整）═══

[1] 波動率目標化（Vol Targeting；文獻: Moreira & Muir 2017; Harvey et al. 2018）
  - position_t = raw_sig_t × min(1, σ_target / σ_{t-1})    （只減倉不加杠桿）
  - σ = 換月調整日報酬（點）的 N 日滾動標準差，N ∈ {20, 60}（僅兩窗，不掃描）
  - σ_target = 2001-2019 期間 σ_N 的中位數（市場性質，全艙共用，不挑選）
  - 部位粒度 1/20（微型契約 TMF 可實作），再平衡帶 0.10：
    同方向且 |Δ| < 0.10 不動作；方向改變或歸零一律執行
  - 成本 1.5 點/邊 × |Δposition|；結算日開盤強平（與全部研究回測一致）
  - 決策規則（各艙與組合分別判定；votes/trend dev 2001-2019、LT dev 2005-2019）：
    採納候選 = (a) dev Sharpe >= 原艙 + 0.10
             且 (b) dev MDD 較原艙淺 >= 20%
             且 (c) 兩半段 Sharpe 皆 >= 原艙同段 - 0.10
    兩個 N 皆合格 → 取 dev MDD 較淺者；再平手 → N=60（換手較低）
  - 即使通過，採納屬使用者決策；2020+ 數字僅附註

[2] 結算日「強平隔日再進」vs「換月持有」的會計成本（純會計，無參數）
  - 兩者交易邊數相同（2 邊）；唯一差異 = 結算日當天次月合約的日內曝險
  - 量化：sum over 結算日 of 進結算日部位 × (次月收盤 - 次月開盤)
  - 正值 = 現行強平做法放棄的點數（不含再進場時點差異，近似）

[3] 夜盤執行時點（描述性；夜盤資料 2017-05-16 起）
  - 現行：T 日收盤訊號 → T+1 日早盤 08:45 開盤執行
  - 替代：T 日 15:00 夜盤開盤執行（即「交易日 T+1 的盤後節」，標記已驗證:
    night_open(T+1) ≈ day_close(T)，中位差 10 點）
  - 量化：sum over 部位變動日 of Δposition × (早盤開盤 - 夜盤開盤)
    正值 = 等到早盤執行的累計代價
  - 僅適用 votes/trend（其輸入於 14:30 前定案）；LT 資料公布時間晚於 15:00，不適用
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5
GRAN = 0.05          # 部位粒度 1/20
BAND = 0.10          # 再平衡帶
SH_IMPROVE = 0.10    # 決策 (a)
MDD_IMPROVE = 0.20   # 決策 (b)
HALF_TOL = 0.10      # 決策 (c)

# ================= 資料 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
near = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price, high_price, low_price, close_price, delivery_month FROM R WHERE rn=1
    ORDER BY trading_date""", conn)
nxt = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price AS o2, close_price AS c2 FROM R WHERE rn=2
    ORDER BY trading_date""", conn)
ngt = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='盤後')
    SELECT trading_date, open_price AS o_ngt FROM R WHERE rn=1
    ORDER BY trading_date""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
lt = pd.read_sql_query("SELECT date, t5_spec_long, t5_spec_short FROM large_trader_tx ORDER BY date", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tpex = pd.read_sql_query("SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn)
conn.close()

near['trading_date'] = pd.to_datetime(near['trading_date'])
nxt['trading_date'] = pd.to_datetime(nxt['trading_date'])
ngt['trading_date'] = pd.to_datetime(ngt['trading_date'])
lt['date'] = pd.to_datetime(lt['date'])
tpex['date'] = pd.to_datetime(tpex['date'])

M = (near.merge(nxt, on='trading_date', how='left')
         .merge(ngt, on='trading_date', how='left')
         .merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date'))
n = len(M)
dt = M['trading_date']
yr = dt.dt.year.values
o = M['open_price'].values.astype(float)
c = M['close_price'].values.astype(float)
o2 = pd.to_numeric(M['o2'], errors='coerce').values.astype(float)
c2 = pd.to_numeric(M['c2'], errors='coerce').values.astype(float)
o_ngt = pd.to_numeric(M['o_ngt'], errors='coerce').values.astype(float)
dm = M['delivery_month'].values
settle = np.zeros(n, dtype=bool)
settle[:-1] = dm[:-1] != dm[1:]
roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]

DEV = (yr >= 2001) & (yr <= 2019)
H1 = (yr >= 2001) & (yr <= 2010)
H2 = (yr >= 2011) & (yr <= 2019)
LT_DEV = (yr >= 2005) & (yr <= 2019)
LT_H1 = (yr >= 2005) & (yr <= 2012)
LT_H2 = (yr >= 2013) & (yr <= 2019)
TEST = (yr >= 2020)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


# ================= 訊號（與 factor_research6 / strategy_signal_v2 完全相同）=================
# 換月調整連續序列
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)

# SMA200 趨勢（多/空手）
sma200 = pd.Series(adjC).rolling(200).mean().values
above = shift1((adjC > sma200).astype(float))
sig_tr = np.where(np.isnan(shift1(sma200)) | np.isnan(above), 0, np.where(above == 1, 1, 0)).astype(float)

# LT_TREND(20)
net5 = (M['t5_spec_long'] - M['t5_spec_short']).values.astype(float)
x1 = shift1(net5 - pd.Series(net5).rolling(20).mean().values)
sig_lt = np.where(np.isnan(x1), 0, np.where(x1 > 0, 1, -1)).astype(float)

# 四票 PIT（在 tpex inner-join 的 V 索引上算，映回 M；無 tpex 日 = 空手）
V = near.merge(tpex, left_on='trading_date', right_on='date', how='inner').drop(columns='date').reset_index(drop=True)
nv = len(V)
vo = V['open_price'].values.astype(float)
vh = V['high_price'].values.astype(float)
vl = V['low_price'].values.astype(float)
vc = V['close_price'].values.astype(float)
V['tpex_ret'] = V['tpex_close'].pct_change()
vlabel = V['open_price'].pct_change().shift(-2).values
V['ovn'] = (V['open_price'] - V['close_price'].shift(1)) / V['close_price'].shift(1) * 100
V['intra'] = (V['close_price'] - V['open_price']) / V['open_price'] * 100
V['tstd20'] = V['tpex_ret'].rolling(20).std()
V['sma60r'] = V['close_price'] / V['close_price'].rolling(60).mean()
irs = V['intra'].rolling(20).std()
s1a = (irs.rolling(30).mean() > irs.rolling(50).mean()).values
s1b = (V['tstd20'].rolling(20).mean() > V['tstd20'].rolling(80).mean()).values
s1c = (V['ovn'].rolling(40).mean() > 0).values
v1ok = (irs.rolling(50).mean().notna() & V['tstd20'].rolling(80).mean().notna() & V['ovn'].rolling(40).mean().notna()).values
s2a = (V['intra'].rolling(40).mean() > V['intra'].rolling(90).mean()).values
s2b = (V['ovn'].rolling(40).mean() > V['ovn'].rolling(80).mean()).values
s2c = (V['ovn'].rolling(40).mean() > V['intra'].rolling(40).mean()).values
v2ok = (V['intra'].rolling(90).mean().notna() & V['ovn'].rolling(80).mean().notna()).values
s3a = (V['intra'].rolling(30).mean() > V['intra'].rolling(90).mean()).values
s3b = (V['ovn'].rolling(40).mean() > V['ovn'].rolling(60).mean()).values
s3c = (V['sma60r'] > 1).values
v3ok = (V['intra'].rolling(90).mean().notna() & V['ovn'].rolling(60).mean().notna() & V['sma60r'].notna()).values
k1 = s1a.astype(int) * 4 + s1b.astype(int) * 2 + s1c.astype(int)
k2 = s2a.astype(int) * 4 + s2b.astype(int) * 2 + s2c.astype(int)
k3 = s3a.astype(int) * 4 + s3b.astype(int) * 2 + s3c.astype(int)


def pit_votes(keys, valid):
    sums = np.zeros(8)
    cnts = np.zeros(8)
    votes = np.zeros(nv, dtype=np.int8)
    for i in range(nv):
        j = i - 2
        if j >= 0 and valid[j] and not np.isnan(vlabel[j]):
            sums[keys[j]] += vlabel[j]
            cnts[keys[j]] += 1
        if valid[i]:
            er = sums[keys[i]] / cnts[keys[i]] if cnts[keys[i]] > 0 else 0.0
            votes[i] = 1 if er > 0 else -1
    return votes


vt1, vt2, vt3 = pit_votes(k1, v1ok), pit_votes(k2, v2ok), pit_votes(k3, v3ok)
us = (vh - np.maximum(vo, vc)) / vo
ir = (vh - vl) / vo
us_l = pd.Series(us).rolling(10).mean().values
ir_l = pd.Series(ir).rolling(10).mean().values
v4ok = ~(np.isnan(us_l) | np.isnan(ir_l))
ts4 = ((pd.Series(us).rolling(2).mean().values > us_l) & v4ok).astype(np.int8) + \
      ((pd.Series(ir).rolling(5).mean().values > ir_l) & v4ok).astype(np.int8)
vt4 = np.where(v4ok, np.where(ts4 == 2, 1, np.where(ts4 == 0, -1, 0)), 0).astype(np.int8)
vtarget = np.where(vt1 + vt2 + vt3 + vt4 >= 2, 1, np.where(vt1 + vt2 + vt3 + vt4 <= -2, -1, 0)).astype(np.int8)
exec_v = np.zeros(nv)
exec_v[1:] = vtarget[:-1]
vmap = dict(zip(V['trading_date'].dt.strftime('%Y-%m-%d'), exec_v))
sig_votes = np.array([vmap.get(d, 0.0) for d in dt.dt.strftime('%Y-%m-%d')])

# ================= 波動率 scale =================
scales = {'raw': np.ones(n)}
for N in (20, 60):
    sig_ma = pd.Series(adj_ret).rolling(N).std().values
    tgt = np.nanmedian(sig_ma[DEV])
    s = np.minimum(1.0, tgt / shift1(sig_ma))
    s[np.isnan(s)] = 1.0
    scales[f'VT{N}'] = s

# ================= 分數部位引擎 =================
def frac_engine(raw, scale):
    """raw[t] = 當日應持方向(由 t-1 資訊決定)；scale[t] 同步。回傳 (pnl, positions)。"""
    pnl = np.zeros(n)
    positions = np.zeros(n)
    pos = 0.0
    for t in range(1, n):
        p_prev = pos
        if settle[t]:
            pnl[t] = (p_prev * (o[t] - c[t - 1]) if not roll_first[t] else 0.0) - COST * abs(p_prev)
            pos = 0.0
            positions[t] = pos
            continue
        tgt = raw[t] * scale[t]
        tgt = round(tgt / GRAN) * GRAN
        if not (np.sign(tgt) != np.sign(pos) or abs(tgt - pos) >= BAND):
            tgt = pos
        ovn = 0.0 if roll_first[t] else p_prev * (o[t] - c[t - 1])
        pnl[t] = ovn + tgt * (c[t] - o[t]) - COST * abs(tgt - p_prev)
        pos = tgt
        positions[t] = pos
    return pnl, positions


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, mdd=mdd, posy=int((ys > 0).sum()), ny=len(ys))


# ================= [1] Vol targeting =================
sleeves = {
    '四票(PIT)': (sig_votes, DEV, H1, H2),
    'SMA200趨勢': (sig_tr, DEV, H1, H2),
    'LT(紙上)': (sig_lt, LT_DEV, LT_H1, LT_H2),
}
print("=" * 104)
print("[1] 波動率目標化  （決策只看 dev；2020+ 描述性。規則: devSh>=raw+0.10 且 devMDD 淺>=20% 且 兩半段不劣 0.10）")
print("=" * 104)
print(f"{'艙/版本':<22} {'devSh':>6} {'devMDD':>8} {'dev淨':>9} {'半1Sh':>6} {'半2Sh':>6} | {'testSh':>6} {'testMDD':>9} {'test淨':>9}")
print("-" * 104)

results = {}
pnl_store = {}
for nm, (raw, dmask, h1m, h2m) in sleeves.items():
    rows = {}
    for sc_nm, sc in scales.items():
        pnl, posn = frac_engine(raw, sc)
        d = stats(pnl, dmask)
        h1s = stats(pnl, h1m)['sharpe']
        h2s = stats(pnl, h2m)['sharpe']
        t = stats(pnl, TEST)
        rows[sc_nm] = (d, h1s, h2s, t)
        pnl_store[(nm, sc_nm)] = (pnl, posn)
        print(f"{nm + ' ' + sc_nm:<22} {d['sharpe']:>6.2f} {d['mdd']:>8,.0f} {d['tot']:>9,.0f} "
              f"{h1s:>6.2f} {h2s:>6.2f} | {t['sharpe']:>6.2f} {t['mdd']:>9,.0f} {t['tot']:>9,.0f}")
    results[nm] = rows
    rd, rh1, rh2, _ = rows['raw']
    verdicts = []
    for N in ('VT20', 'VT60'):
        d, h1s, h2s, _ = rows[N]
        ok = (d['sharpe'] >= rd['sharpe'] + SH_IMPROVE
              and d['mdd'] >= (1 - MDD_IMPROVE) * rd['mdd']
              and h1s >= rh1 - HALF_TOL and h2s >= rh2 - HALF_TOL)
        verdicts.append((N, ok, d['mdd']))
    passed = [v for v in verdicts if v[1]]
    if passed:
        pick = max(passed, key=lambda v: v[2])  # MDD 較淺（較大）者；同值取後者(VT60)
        print(f"    >>> 判定: {pick[0]} 通過採納候選規則")
    else:
        print(f"    >>> 判定: 未過（維持原 1 口）")
    print("-" * 104)

# 組合（四票+趨勢，現行 v2 雙艙）
print(f"{'組合: 四票+趨勢':<104}")
for sc_nm in ('raw', 'VT20', 'VT60'):
    pnl = pnl_store[('四票(PIT)', sc_nm)][0] + pnl_store[('SMA200趨勢', sc_nm)][0]
    d = stats(pnl, DEV)
    h1s = stats(pnl, H1)['sharpe']
    h2s = stats(pnl, H2)['sharpe']
    t = stats(pnl, TEST)
    print(f"{'  combo ' + sc_nm:<22} {d['sharpe']:>6.2f} {d['mdd']:>8,.0f} {d['tot']:>9,.0f} "
          f"{h1s:>6.2f} {h2s:>6.2f} | {t['sharpe']:>6.2f} {t['mdd']:>9,.0f} {t['tot']:>9,.0f}")

# ================= [2] 結算日換月會計 =================
print()
print("=" * 104)
print("[2] 結算日強平 vs 換月持有：現行做法放棄的次月合約日內點數（正=放棄獲利）")
print("=" * 104)
for nm, (raw, dmask, _, _) in sleeves.items():
    posn = pnl_store[(nm, 'raw')][1]
    into = np.zeros(n)
    into[1:] = posn[:-1]
    val = into * (c2 - o2)
    ok = settle & ~np.isnan(val) & (into != 0)
    miss = int((settle & (np.isnan(c2) | np.isnan(o2)) & (into != 0)).sum())
    for lbl, mask in (('dev', dmask), ('test(描述)', TEST)):
        m = ok & mask
        print(f"  {nm:<12} {lbl:<10} 放棄點數合計 {val[m].sum():>+9,.0f}  （{int(m.sum())} 個結算日有部位, 缺次月報價 {miss}）")

# ================= [3] 夜盤執行時點（2017-05-16 起；僅 votes/trend）=================
print()
print("=" * 104)
print("[3] 夜盤(15:00)執行 vs 現行早盤(08:45)執行：等待的累計代價（正=等待吃虧）")
print("=" * 104)
NGT_OK = ~np.isnan(o_ngt)
for nm in ('四票(PIT)', 'SMA200趨勢'):
    posn = pnl_store[(nm, 'raw')][1]
    dpos = np.zeros(n)
    dpos[1:] = posn[1:] - posn[:-1]
    move = (dpos != 0) & NGT_OK & ~settle
    val = dpos * (o - o_ngt)
    for lbl, lo, hi in (('2017/05-2019', 2017, 2019), ('2020-2026(描述)', 2020, 2099)):
        m = move & (yr >= lo) & (yr <= hi)
        tot = val[m].sum()
        cnt = int(m.sum())
        unit = abs(dpos[m]).sum()
        print(f"  {nm:<12} {lbl:<16} 合計 {tot:>+8,.0f} 點 / {cnt:>4} 次變動 / 每單位部位均 {tot / unit if unit else 0:>+6.2f} 點")
print("\n  註: 夜盤開盤流動性較日盤薄，實際滑價可能高於日盤；LT 艙資料公布晚於 15:00 不適用。")
print("\n[done] 規則與參數皆為檔頭事前承諾；無掃描、無事後挑選。")
