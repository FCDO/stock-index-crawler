# -*- coding: utf-8 -*-
"""
因子研究 第4輪：選擇權 P/C ratio + 三大法人籌碼（新資訊維度）
（第1-2輪 22 組 factor_research.py；第3輪 16 組 factor_research2.py；本輪 12 組，累積 50 組）

資料可得性（在看任何因子績效之前確認，據此分軌不構成 snooping）：
- P/C ratio: TAIFEX 完整歷史 2007/07 起 → A 軌（完整統計力）
- 三大法人: TAIFEX 線上僅近3年；FinMind 鏡像自 2018-06 起 → B 軌（統計力不足，門檻加嚴）

事前承諾：
[A 軌] dev 2008-2019（12年），半段 2008-2013 / 2014-2019，test 2020-2026 一次性
       門檻: Sharpe>=0.7、正年>=8/12、家族高原(>=2組Sh>=0.5; 單組自身達標)、兩半段皆正
[B 軌] dev 2018-07~2023-12（5.5年），半段 ~2020-12 / 2021-01~，test 2024-01~2026-06 一次性
       門檻【加嚴】: Sharpe>=1.0、正年>=4/6、家族高原、兩半段皆正
       B 軌即使通過也僅視為「低信度候選」，需實盤紙上追蹤再採用
- 方向先承諾，禁止事後翻號：
  外資淨部位→跟隨；自營商→跟隨（若為負只記錄教訓）；
  P/C 未平倉比高(升)→偏多跟隨；P/C 成交量比極端高=恐慌→逆勢做多
- 成本每邊 1.5 點；引擎與結算日處理與前三輪相同

候選（12 組）：
  A 軌: PCOI(20) PCOI(60) PCVOL(252,80/20)
  B 軌: F_SIGN(raw) F_SIGN(MA5) | F_TREND(20) F_TREND(60) | F_FLOW(5) F_FLOW(20)
        D_SIGN(raw) D_TREND(20) | COMBO(外資+投信)
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5

# ================= 資料 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH RankedData AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) as rn
        FROM tx_futures WHERE session = '一般'
    )
    SELECT trading_date, open_price, close_price, delivery_month
    FROM RankedData WHERE rn = 1 ORDER BY trading_date
""", conn)
conn.close()
df['trading_date'] = pd.to_datetime(df['trading_date'])

conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
inst = pd.read_sql_query("SELECT date, identity, net_vol, net_oi FROM fut_institutional", conn)
pc = pd.read_sql_query("SELECT date, pc_vol_ratio, pc_oi_ratio FROM pc_ratio", conn)
conn.close()

inst['date'] = pd.to_datetime(inst['date'])
pc['date'] = pd.to_datetime(pc['date'])
piv = inst.pivot(index='date', columns='identity', values='net_oi')
piv.columns = [f'{c}_oi' for c in piv.columns]

# 分開合併：P/C 歷史(2007+)比三大法人(2018+)長，不可用三大法人的日期當主索引
df = df.merge(piv.reset_index(), left_on='trading_date', right_on='date', how='left').drop(columns='date')
df = df.merge(pc, left_on='trading_date', right_on='date', how='left').drop(columns='date')

n = len(df)
yr = df['trading_date'].dt.year.values
dt = df['trading_date']
o = df['open_price'].values.astype(float)
c = df['close_price'].values.astype(float)
dm = df['delivery_month'].values
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]

A_DEV = (yr >= 2008) & (yr <= 2019)
A_H1 = (yr >= 2008) & (yr <= 2013)
A_H2 = (yr >= 2014) & (yr <= 2019)
A_TEST = (yr >= 2020)
B_DEV = ((dt >= '2018-07-01') & (dt <= '2023-12-31')).values
B_H1 = ((dt >= '2018-07-01') & (dt <= '2020-12-31')).values
B_H2 = ((dt >= '2021-01-01') & (dt <= '2023-12-31')).values
B_TEST = (dt >= '2024-01-01').values

f_oi = df['foreign_oi'].values.astype(float) if 'foreign_oi' in df else np.full(n, np.nan)
d_oi = df['dealer_oi'].values.astype(float) if 'dealer_oi' in df else np.full(n, np.nan)
t_oi = df['trust_oi'].values.astype(float) if 'trust_oi' in df else np.full(n, np.nan)
pcv = df['pc_vol_ratio'].values.astype(float)
pco = df['pc_oi_ratio'].values.astype(float)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


def ma(x, N):
    return pd.Series(x).rolling(N).mean().values


# ================= 引擎（與前三輪相同）=================
def run_cont(sig, cost=COST):
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


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    tt = x.mean() / sd * np.sqrt(len(x)) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, t=tt, mdd=mdd, ys=ys, posy=int((ys > 0).sum()),
                nyears=len(ys))


def sgn_pos(x1, thr=0.0):
    return np.where(np.isnan(x1), 0, np.where(x1 > thr, 1, -1))


# ================= 候選 =================
candidates = []  # (track, family, name, param, sig)

# --- A 軌: P/C ratio ---
for N in [20, 60]:
    x1 = shift1(pco - ma(pco, N))
    candidates.append(('A', 'PCOI', f'PCOI({N})', N, sgn_pos(x1)))
q80 = pd.Series(pcv).rolling(252).quantile(0.80).values
q20 = pd.Series(pcv).rolling(252).quantile(0.20).values
x1, hi1, lo1 = shift1(pcv), shift1(q80), shift1(q20)
sig = np.where(np.isnan(x1) | np.isnan(hi1), 0,
               np.where(x1 > hi1, 1, np.where(x1 < lo1, -1, 0)))
candidates.append(('A', 'PCVOL', 'PCVOL(252,80/20)', 1, sig))

# --- B 軌: 三大法人 ---
candidates.append(('B', 'F_SIGN', 'F_SIGN(raw)', 1, sgn_pos(shift1(f_oi))))
candidates.append(('B', 'F_SIGN', 'F_SIGN(MA5)', 5, sgn_pos(shift1(ma(f_oi, 5)))))
for N in [20, 60]:
    candidates.append(('B', 'F_TREND', f'F_TREND({N})', N, sgn_pos(shift1(f_oi - ma(f_oi, N)))))
for N in [5, 20]:
    x1 = shift1(f_oi - np.concatenate([np.full(N, np.nan), f_oi[:-N]]))
    candidates.append(('B', 'F_FLOW', f'F_FLOW({N})', N, sgn_pos(x1)))
candidates.append(('B', 'D_SIGN', 'D_SIGN(raw)', 1, sgn_pos(shift1(d_oi))))
candidates.append(('B', 'D_TREND', 'D_TREND(20)', 20, sgn_pos(shift1(d_oi - ma(d_oi, 20)))))
fo1, tr1 = shift1(f_oi), shift1(t_oi)
sig = np.where(np.isnan(fo1) | np.isnan(tr1), 0,
               np.where((fo1 > 0) & (tr1 > 0), 1, np.where((fo1 < 0) & (tr1 < 0), -1, 0)))
candidates.append(('B', 'COMBO', 'COMBO(外資+投信)', 1, sig))

# ================= 評估 =================
pnl_long, _ = run_cont(np.ones(n))

TRACKS = {
    'A': dict(DEV=A_DEV, H1=A_H1, H2=A_H2, TEST=A_TEST, sh_bar=0.7, posy_bar=8,
              label='A軌 P/C ratio  dev 2008-2019, test 2020-2026'),
    'B': dict(DEV=B_DEV, H1=B_H1, H2=B_H2, TEST=B_TEST, sh_bar=1.0, posy_bar=4,
              label='B軌 三大法人   dev 2018-07~2023-12, test 2024-01~2026-06【低信度軌】'),
}

results = {}
for trk, cfg in TRACKS.items():
    bl = stats(pnl_long, cfg['DEV'])
    print("=" * 110)
    print(f"{cfg['label']}")
    print(f"基準[永遠做多] dev: 淨 {bl['tot']:,.0f}  Sharpe {bl['sharpe']:.2f}  正年 {bl['posy']}/{bl['nyears']}")
    print("=" * 110)
    print(f"{'家族':<8} {'設定':<18} {'淨損益':>9} {'Sharpe':>7} {'t值':>6} {'正年':>6} {'前半':>9} {'後半':>9} {'MDD':>8} {'邊數':>6}")
    print("-" * 110)
    for trk2, fam, name, prm, sig in candidates:
        if trk2 != trk:
            continue
        pnl, sides = run_cont(sig)
        st = stats(pnl, cfg['DEV'])
        h1, h2 = stats(pnl, cfg['H1']), stats(pnl, cfg['H2'])
        results[name] = dict(track=trk, family=fam, param=prm, sig=sig, pnl=pnl,
                             dev=st, h1=h1['tot'], h2=h2['tot'], sides=sides)
        print(f"{fam:<8} {name:<18} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['t']:>6.2f} "
              f"{st['posy']:>4}/{st['nyears']} {h1['tot']:>9,.0f} {h2['tot']:>9,.0f} {st['mdd']:>8,.0f} {sides:>6}")
    print()

# ================= 機械式判定 =================
print("=" * 110)
print("存活判定（規則寫死：A軌 Sh>=0.7&正年>=8；B軌 Sh>=1.0&正年>=4；皆需家族高原+兩半段為正；中位參數）")
print("=" * 110)
families = {}
for nm, r in results.items():
    families.setdefault((r['track'], r['family']), []).append(nm)

survivors = {'A': [], 'B': []}
for (trk, fam), names in families.items():
    cfg = TRACKS[trk]
    sh = {nm: results[nm]['dev']['sharpe'] for nm in names}
    best = max(sh, key=sh.get)
    plateau = sum(1 for v in sh.values() if v >= 0.5) >= (2 if len(names) > 1 else 1)
    base_ok = (sh[best] >= cfg['sh_bar'] and results[best]['dev']['posy'] >= cfg['posy_bar'] and plateau)
    if base_ok:
        oks = sorted([x for x in names if sh[x] >= 0.5], key=lambda x: results[x]['param'])
        pick = oks[len(oks) // 2]
        if results[pick]['h1'] > 0 and results[pick]['h2'] > 0:
            survivors[trk].append(pick)
            print(f"[PASS-{trk}] {fam:<8} 入選 {pick} (Sh={sh[pick]:.2f}, 半段 {results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
        else:
            print(f"[FAIL-{trk}] {fam:<8} {pick} 兩半段不一致 ({results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
    else:
        print(f"[FAIL-{trk}] {fam:<8} 最佳 {best}(Sh={sh[best]:.2f}, 正年{results[best]['dev']['posy']}, 高原{'O' if plateau else 'X'})")

# ================= 一次性最終測試 =================
for trk, cfg in TRACKS.items():
    sv = sorted(survivors[trk], key=lambda x: -results[x]['dev']['sharpe'])[:3]
    print(f"\n>>> {trk}軌存活: {sv if sv else '（無）'}")
    if sv:
        print("-" * 110)
        print(f"{trk}軌 一次性最終測試（測試集重複使用中 — 保守解讀）")
        bt = stats(pnl_long, cfg['TEST'])
        for nm in sv:
            st = stats(results[nm]['pnl'], cfg['TEST'])
            print(f"{nm:<20} 淨 {st['tot']:>8,.0f}  Sharpe {st['sharpe']:5.2f}  正年 {st['posy']}/{st['nyears']}  MDD {st['mdd']:>8,.0f}")
            print(f"  年度: " + "  ".join(f"{y}:{v:+,.0f}" for y, v in st['ys'].items()))
        print(f"{'基準: 永遠做多':<20} 淨 {bt['tot']:>8,.0f}  Sharpe {bt['sharpe']:5.2f}  正年 {bt['posy']}/{bt['nyears']}  MDD {bt['mdd']:>8,.0f}")

print("\n[done] 全部 12 組已列出；方向/門檻為事前承諾，無事後翻號或挑選。")
