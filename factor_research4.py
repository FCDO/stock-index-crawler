# -*- coding: utf-8 -*-
"""
因子研究 第5輪：(a) 文獻日曆/期限結構因子  (b) 互補性分析（組合不調權重）
（累積: R1-2=22, R3=16, R4=12, 本輪 4 因子 + 3 組合 = 57 組測試）

事前承諾：
[5a 新因子] dev 2001-2019、半段 2001-2010/2011-2019、test 2020-2026 一次性
  門檻同前: Sharpe>=0.7、正年>=12/19、家族高原(>=2組Sh>=0.5)、兩半段皆正、中位參數
  - TOM 月轉換效應(文獻: McConnell-Xu 等, 跨國穩健): 月末最後1日~次月前N日做多, 其餘空手
    TOM(-1,+3)=4天 / TOM(-1,+2)=3天
  - CALSPREAD 跨月價差(期限結構/carry): z=(次月-近月價差 - 60/120日均)/std
    z<-1(逆價差深於常態)→多, z>+1→空。與第2輪 BASIS 為相關假說(BASIS 已 OOS 死亡), 先承諾不因此加分
[5b 互補性] PnL 日相關矩陣(描述性) + 三個固定組合(雙艙各1口、損益相加, 無權重):
  C0 = v2四票(PIT) + SMA200趨勢      （現行 v2 兩艙是否互補）
  C1 = SMA200趨勢 + PCVOL恐慌買進    （趨勢空手時的崩盤反彈互補假說）
  C2 = v2四票(PIT) + PCVOL恐慌買進
  dev 2008-2019 決策（PCVOL 受 PC 資料 2007/07+252 日暖身限制）:
    採納候選資格 = 組合 dev Sharpe >= max(成分 dev Sharpe)+0.15 且 組合 MDD <= 1.2x較佳成分 MDD
    通過者 test 2020-2026 一次驗證: 組合 test Sharpe >= 較佳成分 test Sharpe 才算確認
  註: PCVOL 單獨未過第4輪門檻(0.64) — 即使組合通過, 定位仍是「紙上追蹤候選」, 非直接部署
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
nxt = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, close_price AS next_close FROM R WHERE rn=2
    ORDER BY trading_date
""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
pc = pd.read_sql_query("SELECT date, pc_vol_ratio FROM pc_ratio ORDER BY date", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tpex = pd.read_sql_query("SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn)
conn.close()

near['trading_date'] = pd.to_datetime(near['trading_date'])
nxt['trading_date'] = pd.to_datetime(nxt['trading_date'])
pc['date'] = pd.to_datetime(pc['date'])
tpex['date'] = pd.to_datetime(tpex['date'])

M = near.merge(nxt, on='trading_date', how='left').merge(
    pc, left_on='trading_date', right_on='date', how='left').drop(columns='date')

n = len(M)
dt = M['trading_date']
yr = dt.dt.year.values
o = M['open_price'].values.astype(float)
h = M['high_price'].values.astype(float)
l = M['low_price'].values.astype(float)
c = M['close_price'].values.astype(float)
c2 = pd.to_numeric(M['next_close'], errors='coerce').values.astype(float)
dm = M['delivery_month'].values
settle = np.zeros(n, dtype=bool)
settle[:-1] = dm[:-1] != dm[1:]
roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]

DEV = (yr >= 2001) & (yr <= 2019)
H1 = (yr >= 2001) & (yr <= 2010)
H2 = (yr >= 2011) & (yr <= 2019)
TEST = (yr >= 2020)
CDEV = (yr >= 2008) & (yr <= 2019)   # 組合分析窗(受 PCVOL 暖身限制)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


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


# ================= 5a 新因子 =================
print("=" * 106)
print("5a 新因子  dev 2001-2019  （門檻: Sh>=0.7, 正年>=12/19, 高原, 兩半段皆正）")
print("=" * 106)
pnl_long = run_cont(np.ones(n), o, c, settle)
bl = stats(pnl_long, DEV)
print(f"基準[永遠做多]: 淨 {bl['tot']:,.0f}  Sharpe {bl['sharpe']:.2f}  正年 {bl['posy']}/19")
print(f"{'家族':<10} {'設定':<16} {'淨損益':>9} {'Sharpe':>7} {'正年':>6} {'前半':>9} {'後半':>9} {'MDD':>8}")
print("-" * 106)

month = dt.dt.month.values
first_m = np.zeros(n, dtype=bool)
first_m[1:] = month[1:] != month[:-1]

cands = []
for last_n, name in [(3, 'TOM(-1,+3)'), (2, 'TOM(-1,+2)')]:
    sig = np.zeros(n)
    idx = np.where(first_m)[0]
    for b in idx:
        sig[max(b - 1, 0): min(b + last_n, n)] = 1
    cands.append(('TOM', name, last_n, sig))

spread = c2 - c
for N in [60, 120]:
    mu = pd.Series(spread).rolling(N).mean().values
    sd = pd.Series(spread).rolling(N).std().values
    z1 = shift1((spread - mu) / sd)
    sig = np.where(np.isnan(z1), 0, np.where(z1 < -1, 1, np.where(z1 > 1, -1, 0)))
    cands.append(('CALSPREAD', f'CALSPREAD({N})', N, sig))

res5a = {}
for fam, name, prm, sig in cands:
    pnl = run_cont(sig, o, c, settle)
    st = stats(pnl, DEV)
    h1, h2 = stats(pnl, H1), stats(pnl, H2)
    res5a[name] = dict(family=fam, param=prm, pnl=pnl, dev=st, h1=h1['tot'], h2=h2['tot'])
    print(f"{fam:<10} {name:<16} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['posy']:>4}/19 "
          f"{h1['tot']:>9,.0f} {h2['tot']:>9,.0f} {st['mdd']:>8,.0f}")

print()
fams = {}
for nm, r in res5a.items():
    fams.setdefault(r['family'], []).append(nm)
surv5a = []
for fam, names in fams.items():
    sh = {nm: res5a[nm]['dev']['sharpe'] for nm in names}
    best = max(sh, key=sh.get)
    plateau = sum(1 for v in sh.values() if v >= 0.5) >= 2
    ok = sh[best] >= 0.7 and res5a[best]['dev']['posy'] >= 12 and plateau
    if ok:
        oks = sorted([x for x in names if sh[x] >= 0.5], key=lambda x: res5a[x]['param'])
        pick = oks[len(oks) // 2]
        if res5a[pick]['h1'] > 0 and res5a[pick]['h2'] > 0:
            surv5a.append(pick)
            print(f"[PASS] {fam} 入選 {pick}")
        else:
            print(f"[FAIL] {fam} {pick} 兩半段不一致")
    else:
        print(f"[FAIL] {fam:<10} 最佳 {best}(Sh={sh[best]:.2f}, 正年{res5a[best]['dev']['posy']}/19, 高原{'O' if plateau else 'X'})")
if surv5a:
    print("\n一次性最終測試 2020-2026:")
    bt = stats(pnl_long, TEST)
    for nm in surv5a:
        st = stats(res5a[nm]['pnl'], TEST)
        print(f"  {nm:<16} 淨 {st['tot']:>8,.0f}  Sharpe {st['sharpe']:5.2f}  正年 {st['posy']}/{st['nyears']}  MDD {st['mdd']:,.0f}")
    print(f"  {'永遠做多':<16} 淨 {bt['tot']:>8,.0f}  Sharpe {bt['sharpe']:5.2f}")

# ================= 5b 互補性 =================
print()
print("=" * 106)
print("5b 互補性分析（組合無權重, 雙艙損益相加; 決策窗 dev 2008-2019, test 2020-2026 一次性）")
print("=" * 106)

# --- 成分1: SMA200 趨勢 ---
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)
sma200 = pd.Series(adjC).rolling(200).mean().values
above = shift1((adjC > sma200).astype(float))
sig_trend = np.where(np.isnan(shift1(sma200)) | np.isnan(above), 0, np.where(above == 1, 1, 0))
pnl_trend = run_cont(sig_trend, o, c, settle)

# --- 成分2: PCVOL 恐慌買進（第4輪定義不變）---
pcv = M['pc_vol_ratio'].values.astype(float)
q80 = pd.Series(pcv).rolling(252).quantile(0.80).values
q20 = pd.Series(pcv).rolling(252).quantile(0.20).values
x1, hi1, lo1 = shift1(pcv), shift1(q80), shift1(q20)
sig_pcvol = np.where(np.isnan(x1) | np.isnan(hi1), 0,
                     np.where(x1 > hi1, 1, np.where(x1 < lo1, -1, 0)))
pnl_pcvol = run_cont(sig_pcvol, o, c, settle)

# --- 成分3: v2 四票制 PIT（與 strategy_signal_v2 相同邏輯, 全歷史）---
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
V['dro'] = V['open_price'].pct_change()
vlabel = V['dro'].shift(-2).values
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
us_s = pd.Series(us).rolling(2).mean().values
us_l = pd.Series(us).rolling(10).mean().values
ir_s = pd.Series(ir).rolling(5).mean().values
ir_l = pd.Series(ir).rolling(10).mean().values
v4ok = ~(np.isnan(us_l) | np.isnan(ir_l))
ts4 = ((us_s > us_l) & v4ok).astype(np.int8) + ((ir_s > ir_l) & v4ok).astype(np.int8)
vt4 = np.where(v4ok, np.where(ts4 == 2, 1, np.where(ts4 == 0, -1, 0)), 0).astype(np.int8)
vtotal = (vt1 + vt2 + vt3 + vt4).astype(np.int8)
vtarget = np.where(vtotal >= 2, 1, np.where(vtotal <= -2, -1, 0)).astype(np.int8)
exec_votes = np.zeros(nv)
exec_votes[1:] = vtarget[:-1]
pnl_votes_vf = run_cont(exec_votes, vo, vc, vsettle)
# 映射回主框架日期軸
vmap = dict(zip(V['trading_date'].dt.strftime('%Y-%m-%d'), pnl_votes_vf))
pnl_votes = np.array([vmap.get(d, 0.0) for d in dt.dt.strftime('%Y-%m-%d')])

# --- 相關矩陣 ---
streams = {'四票(PIT)': pnl_votes, 'SMA200趨勢': pnl_trend, 'PCVOL': pnl_pcvol, '永遠做多': pnl_long}
for wname, wmask in [('dev 2008-2019', CDEV), ('test 2020-2026', TEST)]:
    print(f"\nPnL 日相關矩陣（{wname}）:")
    names = list(streams)
    mat = np.corrcoef([streams[nm][wmask] for nm in names])
    print("            " + "".join(f"{nm:>12}" for nm in names))
    for i, nm in enumerate(names):
        print(f"{nm:>12}" + "".join(f"{mat[i][j]:>12.2f}" for j in range(len(names))))

# --- 組合 ---
print()
print(f"{'組合/成分':<24} {'dev淨':>9} {'devSh':>6} {'devMDD':>8} | {'test淨':>9} {'testSh':>7} {'testMDD':>8}")
print("-" * 106)


def row(nm, pnl):
    d, t = stats(pnl, CDEV), stats(pnl, TEST)
    print(f"{nm:<24} {d['tot']:>9,.0f} {d['sharpe']:>6.2f} {d['mdd']:>8,.0f} | "
          f"{t['tot']:>9,.0f} {t['sharpe']:>7.2f} {t['mdd']:>8,.0f}")
    return d, t


dv, tv = row('成分: 四票(PIT)', pnl_votes)
dtr, ttr = row('成分: SMA200趨勢', pnl_trend)
dpc, tpc = row('成分: PCVOL', pnl_pcvol)
row('基準: 永遠做多', pnl_long)
print("-" * 106)
combos = {
    'C0 四票+趨勢': (pnl_votes + pnl_trend, [dv, dtr], [tv, ttr]),
    'C1 趨勢+PCVOL': (pnl_trend + pnl_pcvol, [dtr, dpc], [ttr, tpc]),
    'C2 四票+PCVOL': (pnl_votes + pnl_pcvol, [dv, dpc], [tv, tpc]),
}
for nm, (pnl, dcomp, tcomp) in combos.items():
    d, t = row(nm, pnl)
    best_d = max(x['sharpe'] for x in dcomp)
    best_mdd = max(x['mdd'] for x in dcomp)   # mdd 為負值, max=較淺
    dev_pass = d['sharpe'] >= best_d + 0.15 and d['mdd'] >= 1.2 * best_mdd
    best_t = max(x['sharpe'] for x in tcomp)
    verdict = 'dev未過'
    if dev_pass:
        verdict = 'dev過, test確認' if t['sharpe'] >= best_t else 'dev過, test未確認'
    print(f"{'':<24} 判定: {verdict}  (dev門檻 Sh>={best_d + 0.15:.2f}, MDD>={1.2 * best_mdd:,.0f})")

print("\n[done] 第5輪: 4 因子 + 3 組合, 規則事前承諾。累積測試 57 組。")
