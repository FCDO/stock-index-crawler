# -*- coding: utf-8 -*-
"""
因子研究 第3輪：傳統技術指標 + 量能因子
（第1-2輪見 factor_research.py：趨勢/跳空/價差/相對強弱/票4/波動門檻，22 組）

事前承諾（看結果前寫死）：
- 指標參數用約定俗成的經典值（RSI 14、MACD 12-26-9、KD 9-3-3、海龜 20/55、BB 20/2σ），不掃參數
- 指標一律以「換月調整後」的 OHLC 計算（防基差跳動污染），成交量用原始日盤量
- 存活標準同前（開發期 2001-2019 淨 Sharpe>=0.7、>=12/19 年正、家族高原），
  本輪【加嚴】：入選設定還必須 2001-2010 與 2011-2019 兩半段皆為正（防單一行情撐全場）
- 通過者才做 2020-2026 一次性測試；失敗者全部照列
- 累積測試數已達 ~38 組：任何邊緣性「通過」都應再打折看待

本輪候選（9 家族、16 組）：
  RSI 逆勢     (2,10/90) (7,30/70) (14,30/70)    超賣買/超買空，Connors RSI2 與經典 RSI
  KD           K<20 買 / K>80 空；K-D 交叉順勢    台股散戶最常用，9-3-3
  CCI          20 日，±100 逆勢
  MACD         12-26-9 柱狀體符號，多/空 與 多/空手
  BB 逆勢      20 日，k=2 與 k=2.5，下軌買/上軌空
  DONCHIAN     20 / 55 日通道突破持有（海龜）
  OBV          OBV vs 其 20 日均線，多/空 與 多/空手   （量能維度首次測試）
  VOLSPIKE     爆量(>2x20日均量)下跌日 → 次日做多 1 天（恐慌反轉）
  STR          3 日報酬反轉（-sign(ret3)）
不納入與理由：W%R/隨機指標重複（=KD 之 K）、K線型態（無限組合純挖掘）、
ATR 波動門檻（第2輪已測波動管理失敗）、日曆效應（脆弱）。
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
    SELECT trading_date, open_price, high_price, low_price, close_price, volume, delivery_month
    FROM RankedData WHERE rn = 1 ORDER BY trading_date
""", conn)
conn.close()

df['trading_date'] = pd.to_datetime(df['trading_date'])
n = len(df)
dates = df['trading_date']
yr = dates.dt.year.values
o = df['open_price'].values.astype(float)
h = df['high_price'].values.astype(float)
l = df['low_price'].values.astype(float)
c = df['close_price'].values.astype(float)
vol = df['volume'].values.astype(float)
dm = df['delivery_month'].values

roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]

# 換月調整 OHLC（同日幾何不變，跨月跳動消除）
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
aC = c[0] + np.cumsum(adj_ret)
off = aC - c
aO, aH, aL = o + off, h + off, l + off

DEV = (yr >= 2001) & (yr <= 2019)
H1 = (yr >= 2001) & (yr <= 2010)
H2 = (yr >= 2011) & (yr <= 2019)
TEST = (yr >= 2020)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


# ================= 引擎（與第1-2輪相同）=================
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
    return dict(tot=x.sum(), sharpe=sh, t=tt, mdd=mdd, ys=ys, posy=int((ys > 0).sum()))


# ================= 指標 =================
candidates = []

# --- RSI (Wilder) ---
def rsi(series, N):
    d = np.diff(series, prepend=series[0])
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    au = pd.Series(up).ewm(alpha=1 / N, adjust=False).mean().values
    ad = pd.Series(dn).ewm(alpha=1 / N, adjust=False).mean().values
    rs = np.divide(au, ad, out=np.full(n, np.inf), where=ad > 0)
    return 100 - 100 / (1 + rs)

for N, lo, hi in [(2, 10, 90), (7, 30, 70), (14, 30, 70)]:
    r1 = shift1(rsi(aC, N))
    sig = np.where(np.isnan(r1), 0, np.where(r1 < lo, 1, np.where(r1 > hi, -1, 0)))
    sig[:N + 5] = 0
    candidates.append(('RSI', f'RSI({N},{lo}/{hi})', N, sig))

# --- KD 9-3-3 ---
hh = pd.Series(aH).rolling(9).max().values
ll = pd.Series(aL).rolling(9).min().values
rsv = np.where(hh > ll, (aC - ll) / (hh - ll) * 100, 50.0)
K = np.full(n, 50.0)
D = np.full(n, 50.0)
for t in range(1, n):
    K[t] = K[t - 1] * 2 / 3 + rsv[t] / 3
    D[t] = D[t - 1] * 2 / 3 + K[t] / 3
K1, D1 = shift1(K), shift1(D)
sig = np.where(np.isnan(K1), 0, np.where(K1 < 20, 1, np.where(K1 > 80, -1, 0)))
sig[:15] = 0
candidates.append(('KD', 'KD(K<20/K>80)', 1, sig))
sig = np.where(np.isnan(K1), 0, np.where(K1 > D1, 1, -1))
sig[:15] = 0
candidates.append(('KD', 'KD(K-D交叉)', 2, sig))

# --- CCI 20 ---
tp = (aH + aL + aC) / 3
tps = pd.Series(tp)
ma = tps.rolling(20).mean()
md = tps.rolling(20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
cci = ((tps - ma) / (0.015 * md)).values
c1 = shift1(cci)
sig = np.where(np.isnan(c1), 0, np.where(c1 < -100, 1, np.where(c1 > 100, -1, 0)))
candidates.append(('CCI', 'CCI(20,±100)', 1, sig))

# --- MACD 12-26-9 ---
ema12 = pd.Series(aC).ewm(span=12, adjust=False).mean()
ema26 = pd.Series(aC).ewm(span=26, adjust=False).mean()
dif = ema12 - ema26
dea = dif.ewm(span=9, adjust=False).mean()
hist1 = shift1((dif - dea).values)
hist_ok = np.arange(n) > 40
sig = np.where(~hist_ok | np.isnan(hist1), 0, np.where(hist1 > 0, 1, -1))
candidates.append(('MACD', 'MACD(LS)', 1, sig))
sig = np.where(~hist_ok | np.isnan(hist1), 0, np.where(hist1 > 0, 1, 0))
candidates.append(('MACD', 'MACD(LF)', 2, sig))

# --- BB 逆勢 20 日 ---
mid = pd.Series(aC).rolling(20).mean().values
sd20 = pd.Series(aC).rolling(20).std().values
for kk in [2.0, 2.5]:
    upz = shift1((aC > mid + kk * sd20).astype(float))
    loz = shift1((aC < mid - kk * sd20).astype(float))
    valid = ~np.isnan(shift1(sd20))
    sig = np.where(~valid, 0, np.where(loz == 1, 1, np.where(upz == 1, -1, 0)))
    candidates.append(('BB_MR', f'BB_MR(20,{kk}σ)', kk, sig))

# --- DONCHIAN 海龜通道（突破持有至反向突破）---
for N in [20, 55]:
    hiN = pd.Series(aH).rolling(N).max().values
    loN = pd.Series(aL).rolling(N).min().values
    state = np.zeros(n)
    st = 0
    for t in range(N + 1, n):
        if aC[t - 1] >= hiN[t - 2]:      # 昨收破前 N 日高
            st = 1
        elif aC[t - 1] <= loN[t - 2]:    # 昨收破前 N 日低
            st = -1
        state[t] = st
    candidates.append(('DONCH', f'DONCH({N})', N, state))

# --- OBV vs 20 日均 ---
obv = np.cumsum(np.sign(np.diff(aC, prepend=aC[0])) * vol)
obv_ma = pd.Series(obv).rolling(20).mean().values
ob1 = shift1(obv - obv_ma)
valid = ~np.isnan(ob1)
sig = np.where(~valid, 0, np.where(ob1 > 0, 1, -1))
candidates.append(('OBV', 'OBV(LS)', 1, sig))
sig = np.where(~valid, 0, np.where(ob1 > 0, 1, 0))
candidates.append(('OBV', 'OBV(LF)', 2, sig))

# --- VOLSPIKE 爆量下跌 → 次日做多 1 天 ---
vma = pd.Series(vol).rolling(20).mean().values
ret1 = np.diff(aC, prepend=aC[0])
cond = (vol > 2 * np.concatenate([[np.nan], vma[:-1]])) & (ret1 < 0)
sig = np.where(shift1(cond.astype(float)) == 1, 1, 0)
candidates.append(('VOLSPK', 'VOLSPIKE(2x,跌)', 1, sig))

# --- STR 3 日反轉 ---
r3 = shift1(aC - np.concatenate([np.full(3, np.nan), aC[:-3]]))
sig = np.where(np.isnan(r3), 0, np.where(r3 > 0, -1, 1))
candidates.append(('STR', 'STR(3日反轉)', 1, sig))

# ================= 評估 =================
pnl_long, _ = run_cont(np.ones(n))
bl = stats(pnl_long, DEV)
print("=" * 104)
print(f"第3輪 開發樣本 2001-2019   基準[永遠做多]: 淨 {bl['tot']:,.0f}  Sharpe {bl['sharpe']:.2f}  正年 {bl['posy']}/19")
print(f"(累積測試數: 第1-2輪 22 組 + 本輪 {len(candidates)} 組)")
print("=" * 104)
print(f"{'家族':<8} {'設定':<18} {'淨損益':>9} {'Sharpe':>7} {'t值':>6} {'正年':>6} {'前半01-10':>9} {'後半11-19':>9} {'MDD':>8} {'邊數':>6}")
print("-" * 104)

results = {}
for fam, name, prm, sig in candidates:
    pnl, sides = run_cont(sig)
    st = stats(pnl, DEV)
    h1, h2 = stats(pnl, H1), stats(pnl, H2)
    results[name] = dict(family=fam, param=prm, sig=sig, pnl=pnl, dev=st,
                         h1=h1['tot'], h2=h2['tot'], sides=sides)
    print(f"{fam:<8} {name:<18} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['t']:>6.2f} "
          f"{st['posy']:>4}/19 {h1['tot']:>9,.0f} {h2['tot']:>9,.0f} {st['mdd']:>8,.0f} {sides:>6}")

# ================= 機械式判定（含本輪加嚴條款） =================
print()
print("=" * 104)
print("判定: Sharpe>=0.7 且 正年>=12/19 且 家族高原(>=2組Sh>=0.5, 單組家族自身達標) 且【兩半段皆為正】")
print("=" * 104)
families = {}
for nm, r in results.items():
    families.setdefault(r['family'], []).append(nm)

survivors = []
for fam, names in families.items():
    sh = {nm: results[nm]['dev']['sharpe'] for nm in names}
    best = max(sh, key=sh.get)
    plateau = sum(1 for v in sh.values() if v >= 0.5) >= (2 if len(names) > 1 else 1)
    base_ok = sh[best] >= 0.7 and results[best]['dev']['posy'] >= 12 and plateau
    if base_ok:
        oks = sorted([x for x in names if sh[x] >= 0.5], key=lambda x: results[x]['param'])
        pick = oks[len(oks) // 2]
        halves_ok = results[pick]['h1'] > 0 and results[pick]['h2'] > 0
        if halves_ok:
            survivors.append(pick)
            print(f"[PASS] {fam:<8} 入選 {pick} (Sh={sh[pick]:.2f}, 兩半段 {results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
        else:
            print(f"[FAIL] {fam:<8} {pick} 過基本門檻但兩半段不一致 ({results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
    else:
        print(f"[FAIL] {fam:<8} 最佳 {best}(Sh={sh[best]:.2f}, 正年{results[best]['dev']['posy']}/19, 高原{'O' if plateau else 'X'})")

survivors = sorted(survivors, key=lambda x: -results[x]['dev']['sharpe'])[:3]
print(f"\n>>> 本輪存活: {survivors if survivors else '（無）'}")

if survivors:
    print()
    print("=" * 104)
    print("一次性最終測試 2020-2026/06（注意：測試集已是第二次使用，邊緣結果應再打折）")
    print("=" * 104)
    bt = stats(pnl_long, TEST)
    for nm in survivors:
        st = stats(results[nm]['pnl'], TEST)
        print(f"{nm:<20} 淨 {st['tot']:>8,.0f}  Sharpe {st['sharpe']:5.2f}  正年 {st['posy']}/7  MDD {st['mdd']:>8,.0f}")
        print(f"  年度: " + "  ".join(f"{y}:{v:+,.0f}" for y, v in st['ys'].items()))
    print(f"{'基準: 永遠做多':<20} 淨 {bt['tot']:>8,.0f}  Sharpe {bt['sharpe']:5.2f}  正年 {bt['posy']}/7  MDD {bt['mdd']:>8,.0f}")

print("\n[done] 本輪全部設定已列出，無事後挑選；參數為事前承諾之經典值。")
