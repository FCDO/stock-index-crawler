# -*- coding: utf-8 -*-
"""
附加分析：大額交易人(LT) 與現有 v2 兩艙的互補性（累積測試 65 + 3 組合 = 68）

事前承諾：
- LT 設定沿用第6輪中位參數規則 → LT_TREND(20)（淨部位 vs 20日均, 跟隨）
- 組合 = 各艙獨立 1 口、損益相加，無權重：
    C3 = SMA200趨勢 + LT
    C4 = 四票(PIT) + LT
    C5 = 四票(PIT) + SMA200趨勢 + LT
- 決策窗 dev 2005-2019（LT 資料 2004-07 起 + 暖身）；半段 2005-2012 / 2013-2019
- dev 通過資格 = 組合 Sharpe >= max(成分 Sharpe)+0.15 且 組合 MDD 不比最淺成分深 20%
- 只有 dev 通過的組合才打開 2020-2026 測試窗（test 確認 = 組合 testSh >= 最佳成分 testSh）
- 若全數 dev 未過：不揭露任何 test 數字（含相關性），LT 測試窗保持乾淨留給未來重驗
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5

# ================= 資料 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
near = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price, high_price, low_price, close_price, delivery_month FROM R WHERE rn=1
    ORDER BY trading_date
""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
lt = pd.read_sql_query("SELECT date, t5_spec_long, t5_spec_short FROM large_trader_tx ORDER BY date", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tpex = pd.read_sql_query("SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn)
conn.close()

near['trading_date'] = pd.to_datetime(near['trading_date'])
lt['date'] = pd.to_datetime(lt['date'])
tpex['date'] = pd.to_datetime(tpex['date'])
M = near.merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date')

n = len(M)
dt = M['trading_date']
yr = dt.dt.year.values
o = M['open_price'].values.astype(float)
c = M['close_price'].values.astype(float)
dm = M['delivery_month'].values
settle = np.zeros(n, dtype=bool)
settle[:-1] = dm[:-1] != dm[1:]
roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]

DEV = (yr >= 2005) & (yr <= 2019)
H1 = (yr >= 2005) & (yr <= 2012)
H2 = (yr >= 2013) & (yr <= 2019)
TEST = (yr >= 2020)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


def ma(x, N):
    return pd.Series(x).rolling(N).mean().values


def run_cont(sig, oo, cc, st, cost=COST):
    nn = len(oo)
    pnl = np.zeros(nn)
    pos = 0
    ep = np.nan
    for t in range(1, nn):
        p = 0.0
        tc = 0.0
        if st[t]:
            if pos == 1:
                p = oo[t] - ep
            elif pos == -1:
                p = ep - oo[t]
            if pos != 0:
                tc = cost
            pos = 0
            ep = np.nan
        else:
            tgt = 0 if np.isnan(sig[t]) else int(sig[t])
            if tgt != pos:
                if pos == 1:
                    p += oo[t] - ep
                elif pos == -1:
                    p += ep - oo[t]
                tc += cost * abs(tgt - pos)
                pos = tgt
                ep = oo[t] if pos != 0 else np.nan
            if pos == 1:
                p += cc[t] - ep
                ep = cc[t]
            elif pos == -1:
                p += ep - cc[t]
                ep = cc[t]
        pnl[t] = p - tc
    return pnl


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, mdd=mdd, posy=int((ys > 0).sum()), nyears=len(ys), ys=ys)


# ================= 成分 =================
# LT_TREND(20)
net5 = (M['t5_spec_long'] - M['t5_spec_short']).values.astype(float)
x1 = shift1(net5 - ma(net5, 20))
sig_lt = np.where(np.isnan(x1), 0, np.where(x1 > 0, 1, -1))
pnl_lt = run_cont(sig_lt, o, c, settle)

# SMA200 趨勢
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)
sma200 = pd.Series(adjC).rolling(200).mean().values
above = shift1((adjC > sma200).astype(float))
sig_tr = np.where(np.isnan(shift1(sma200)) | np.isnan(above), 0, np.where(above == 1, 1, 0))
pnl_tr = run_cont(sig_tr, o, c, settle)

# 四票 PIT（與 strategy_signal_v2 同邏輯）
V = near.merge(tpex[['date', 'tpex_close']], left_on='trading_date', right_on='date', how='inner').drop(columns='date')
V = V.reset_index(drop=True)
nv = len(V)
vo = V['open_price'].values.astype(float)
vh = V['high_price'].values.astype(float)
vl = V['low_price'].values.astype(float)
vc = V['close_price'].values.astype(float)
vdm = V['delivery_month'].values
vsettle = np.zeros(nv, dtype=bool)
vsettle[:-1] = vdm[:-1] != vdm[1:]
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
exec_votes = np.zeros(nv)
exec_votes[1:] = vtarget[:-1]
pnl_votes_vf = run_cont(exec_votes, vo, vc, vsettle)
vmap = dict(zip(V['trading_date'].dt.strftime('%Y-%m-%d'), pnl_votes_vf))
pnl_votes = np.array([vmap.get(d, 0.0) for d in dt.dt.strftime('%Y-%m-%d')])

pnl_long = run_cont(np.ones(n), o, c, settle)

# ================= dev 分析 =================
print("=" * 100)
print("LT 互補性分析  dev 2005-2019（test 窗只在 dev 通過時打開）")
print("=" * 100)
streams = {'四票(PIT)': pnl_votes, 'SMA200趨勢': pnl_tr, 'LT_TREND(20)': pnl_lt, '永遠做多': pnl_long}
names = list(streams)
mat = np.corrcoef([streams[nm][DEV] for nm in names])
print("\nPnL 日相關矩陣（dev 2005-2019）:")
print("              " + "".join(f"{nm:>13}" for nm in names))
for i, nm in enumerate(names):
    print(f"{nm:>13}" + "".join(f"{mat[i][j]:>13.2f}" for j in range(len(names))))

print(f"\n{'成分/組合':<26} {'dev淨':>9} {'devSh':>6} {'devMDD':>8} {'正年':>6} {'前半':>9} {'後半':>9}")
print("-" * 100)


def devrow(nm, pnl):
    d = stats(pnl, DEV)
    h1, h2 = stats(pnl, H1), stats(pnl, H2)
    print(f"{nm:<26} {d['tot']:>9,.0f} {d['sharpe']:>6.2f} {d['mdd']:>8,.0f} "
          f"{d['posy']:>4}/{d['nyears']} {h1['tot']:>9,.0f} {h2['tot']:>9,.0f}")
    return d


dv = devrow('成分: 四票(PIT)', pnl_votes)
dtr = devrow('成分: SMA200趨勢', pnl_tr)
dlt = devrow('成分: LT_TREND(20)', pnl_lt)
devrow('基準: 永遠做多', pnl_long)
print("-" * 100)

combos = {
    'C3 趨勢+LT': (pnl_tr + pnl_lt, [dtr, dlt], [pnl_tr, pnl_lt]),
    'C4 四票+LT': (pnl_votes + pnl_lt, [dv, dlt], [pnl_votes, pnl_lt]),
    'C5 四票+趨勢+LT': (pnl_votes + pnl_tr + pnl_lt, [dv, dtr, dlt], [pnl_votes, pnl_tr, pnl_lt]),
}
passed = {}
for nm, (pnl, dcomp, comps) in combos.items():
    d = devrow(nm, pnl)
    bar_sh = max(x['sharpe'] for x in dcomp) + 0.15
    bar_mdd = 1.2 * max(x['mdd'] for x in dcomp)
    ok = d['sharpe'] >= bar_sh and d['mdd'] >= bar_mdd
    print(f"{'':<26} 判定: {'dev通過' if ok else 'dev未過'}  (需 Sh>={bar_sh:.2f} 且 MDD>={bar_mdd:,.0f})")
    if ok:
        passed[nm] = (pnl, comps, dcomp)

# ================= test（僅 dev 通過者）=================
if passed:
    print()
    print("=" * 100)
    print("一次性最終測試 2020-2026/06（dev 通過的組合及其成分）")
    print("=" * 100)
    for nm, (pnl, comps, dcomp) in passed.items():
        t = stats(pnl, TEST)
        tcomps = [stats(p, TEST) for p in comps]
        best_t = max(x['sharpe'] for x in tcomps)
        print(f"{nm:<20} test 淨 {t['tot']:>8,.0f}  Sh {t['sharpe']:5.2f}  MDD {t['mdd']:>8,.0f}"
              f"  | 成分最佳 testSh {best_t:.2f} → {'確認' if t['sharpe'] >= best_t else '未確認'}")
        print(f"  年度: " + "  ".join(f"{y}:{v:+,.0f}" for y, v in t['ys'].items()))
        for lbl, tc in zip(['成分1', '成分2', '成分3'], tcomps):
            print(f"  {lbl}: test 淨 {tc['tot']:,.0f} Sh {tc['sharpe']:.2f} MDD {tc['mdd']:,.0f}")
else:
    print("\n>>> 三組合 dev 全數未過 — 依承諾不開測試窗，LT 的 2020-2026 保持乾淨留給未來重驗。")

print("\n[done] 累積 68 組。")
