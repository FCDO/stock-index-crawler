# -*- coding: utf-8 -*-
"""
因子研究 第7輪：美股領先（S&P 500 → 台指期）— 封槍後第 1 次修約

═══ 修約記錄 ═══
第6輪（factor_research5.py）承諾「不再追加日線因子輪次」。本輪為使用者於
2026-06-12 明示授權的修約，範圍嚴格限定下列 4 組設定；本輪結束後重新封槍。
累積測試數 68 → 72：任何邊緣性通過都應再打折看待。

═══ 事前承諾（資料健全性驗證完成後、看任何因子績效前寫定）═══

假說（文獻：Rapach, Strauss & Zhou 2013 — 美股對各國股市有滯後預測力；
gradual information diffusion）。注意不利先驗：第1輪 GAP 家族（同假說之
台指跳空代理變數，當沖形式）為全場最差（dev Sharpe -0.14 ~ -0.25）。

資料：^GSPC 日線（us_market.db，1990+，Yahoo chart API）。
時序對齊（已驗證）：美股日期 U 收盤完成於台灣時間 U+1 清晨 04:00-05:00，
早於台指 08:45 開盤 → 台股交易日 D 可用之最新美股資料為 U <= D-1（merge_asof）。
驗證記錄：staleness 週二~五=1天、週一=3天、假日尾端正常；
corr(台指隔夜跳空, 對齊後美股日報酬) 2001+ = 0.659（符合典型事實）。
staleness 處理：一律沿用最新可得美股值（無額外參數）。

候選（1 家族 3 組 + 1 單組家族 = 4 組；方向承諾「跟隨」，禁止事後翻號）：
  USMOM(k)  sign(美股 k 日報酬) → 台指多/空連續持有, k in {5,10,20}   擴散假說（新）
  USD1      sign(美股 1 日報酬) → 台指當日開盤進收盤出（當沖）        直接外溢（GAP 之精煉）

門檻（較前六輪加嚴；2020-2026 窗已多次揭露故 test 僅作否決閘）：
  dev 2001-2019：淨 Sharpe >= 0.8（原 0.7）、正年 >= 12/19、
  兩半段（2001-2010 / 2011-2019）Sharpe 皆 >= 0.3（原僅要求皆正）、
  USMOM 家族高原 >= 2 組 Sh >= 0.5 且合格參數取中位；USD1 須自身全數達標
  dev 通過 → 開 2020-2026 一次性 test，唯 test 僅有否決權：
    test Sharpe < 0.3 → 否決；>= 0.3 → 進 signal_ledger 紙上追蹤，
    滿 12 個月 live Sharpe >= 0.3 方可考慮以半口部署（與 LT 同模式）
  dev 未過 → 不開 test。本輪無論結果皆不直接部署任何東西。

成本每邊 1.5 點；引擎、結算日強平與第1-6輪完全相同。
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
us = pd.read_sql_query("SELECT date, close FROM spx_daily ORDER BY date",
                       sqlite3.connect(os.path.join(BASE, 'us_market.db')))

df['trading_date'] = pd.to_datetime(df['trading_date'])
us['date'] = pd.to_datetime(us['date'])

# 美股報酬（美股日曆上先算好，再 asof 對齊）
us['ret1'] = us['close'].pct_change()
for K in (5, 10, 20):
    us[f'mom{K}'] = us['close'].pct_change(K)
us = us.rename(columns={'date': 'us_date'})
us['avail_from'] = us['us_date'] + pd.Timedelta(days=1)   # U 日資料自 U+1 起可用

df = pd.merge_asof(df.sort_values('trading_date'), us.sort_values('avail_from'),
                   left_on='trading_date', right_on='avail_from', direction='backward')

n = len(df)
yr = df['trading_date'].dt.year.values
o = df['open_price'].values.astype(float)
c = df['close_price'].values.astype(float)
dm = df['delivery_month'].values
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]

DEV = (yr >= 2001) & (yr <= 2019)
H1 = (yr >= 2001) & (yr <= 2010)
H2 = (yr >= 2011) & (yr <= 2019)
TEST = (yr >= 2020)


# ================= 引擎（與第1-6輪相同）=================
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


def run_intra(sig, cost=COST):
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
    return dict(tot=x.sum(), sharpe=sh, t=tt, mdd=mdd, posy=int((ys > 0).sum()))


# ================= 候選訊號 =================
# 對齊已保證「開盤前已知」（U <= D-1），故 sig[t] 即當日應持部位，無需再 shift。
candidates = []
for K in (5, 10, 20):
    x = df[f'mom{K}'].values.astype(float)
    sig = np.where(np.isnan(x), np.nan, np.where(x > 0, 1, -1))
    candidates.append(('USMOM', f'USMOM({K})', K, 'cont', sig))
x = df['ret1'].values.astype(float)
sig = np.where(np.isnan(x), np.nan, np.where(x > 0, 1, -1))
candidates.append(('USD1', 'USD1', 1, 'intra', sig))

# ================= 基準與 dev 結果 =================
pnl_long, _ = run_cont(np.ones(n))
bd = stats(pnl_long, DEV)
print("=" * 108)
print(f"第7輪（修約輪）dev 2001-2019   基準[永遠做多]: 淨 {bd['tot']:,.0f}  Sharpe {bd['sharpe']:.2f}  正年 {bd['posy']}/19")
print(f"(累積測試: 前六輪 68 + 本輪 4 = 72)")
print("=" * 108)
print(f"{'家族':<7} {'設定':<11} {'淨損益':>9} {'Sharpe':>7} {'t值':>6} {'正年':>6} {'半1Sh':>6} {'半2Sh':>6} {'MDD':>8} {'邊數':>6}")
print("-" * 108)

results = {}
for fam, name, prm, eng, sig in candidates:
    pnl, sides = (run_cont(sig) if eng == 'cont' else run_intra(sig))
    d = stats(pnl, DEV)
    h1s = stats(pnl, H1)['sharpe']
    h2s = stats(pnl, H2)['sharpe']
    results[name] = dict(family=fam, param=prm, engine=eng, pnl=pnl, dev=d, h1=h1s, h2=h2s)
    print(f"{fam:<7} {name:<11} {d['tot']:>9,.0f} {d['sharpe']:>7.2f} {d['t']:>6.2f} "
          f"{d['posy']:>4}/19 {h1s:>6.2f} {h2s:>6.2f} {d['mdd']:>8,.0f} {sides:>6}")

# ================= 機械式判定（規則寫死於檔頭）=================
print()
print("=" * 108)
print("判定: dev Sh>=0.8 且 正年>=12/19 且 兩半段 Sh>=0.3 且 USMOM 高原(>=2組Sh>=0.5, 中位參數) / USD1 自身達標")
print("=" * 108)

survivors = []
mom_names = [nm for nm in results if results[nm]['family'] == 'USMOM']
mom_sh = {nm: results[nm]['dev']['sharpe'] for nm in mom_names}
best = max(mom_sh, key=mom_sh.get)
rb = results[best]
plateau = sum(1 for v in mom_sh.values() if v >= 0.5) >= 2
ok = (mom_sh[best] >= 0.8 and rb['dev']['posy'] >= 12 and rb['h1'] >= 0.3 and rb['h2'] >= 0.3 and plateau)
if ok:
    quals = sorted([nm for nm in mom_names if mom_sh[nm] >= 0.5], key=lambda x: results[x]['param'])
    pick = quals[len(quals) // 2]
    survivors.append(pick)
    print(f"[PASS] USMOM  最佳 {best}(Sh={mom_sh[best]:.2f})  入選(中位參數)={pick}")
else:
    print(f"[FAIL] USMOM  最佳 {best}(Sh={mom_sh[best]:.2f}, 正年{rb['dev']['posy']}/19, "
          f"半段{rb['h1']:.2f}/{rb['h2']:.2f}, 高原{'O' if plateau else 'X'})")

r1 = results['USD1']
ok1 = (r1['dev']['sharpe'] >= 0.8 and r1['dev']['posy'] >= 12 and r1['h1'] >= 0.3 and r1['h2'] >= 0.3)
if ok1:
    survivors.append('USD1')
    print(f"[PASS] USD1   Sh={r1['dev']['sharpe']:.2f}")
else:
    print(f"[FAIL] USD1   Sh={r1['dev']['sharpe']:.2f}, 正年{r1['dev']['posy']}/19, 半段{r1['h1']:.2f}/{r1['h2']:.2f}")

# ================= 一次性 test（僅否決閘；dev 未過不開）=================
if survivors:
    print()
    print("=" * 108)
    print("一次性最終測試 2020-2026/06（僅否決閘: testSh<0.3 否決; >=0.3 進 ledger 紙上追蹤 12 個月）")
    print("=" * 108)
    bt = stats(pnl_long, TEST)
    for nm in survivors:
        t = stats(results[nm]['pnl'], TEST)
        verdict = '否決' if t['sharpe'] < 0.3 else '進入紙上追蹤（不部署）'
        print(f"{nm:<11} test 淨 {t['tot']:>8,.0f}  Sh {t['sharpe']:5.2f}  正年 {t['posy']}/7  MDD {t['mdd']:>8,.0f}  -> {verdict}")
    print(f"{'基準:永遠做多':<11} test 淨 {bt['tot']:>8,.0f}  Sh {bt['sharpe']:5.2f}  正年 {bt['posy']}/7  MDD {bt['mdd']:>8,.0f}")
else:
    print("\n>>> 全數 dev 未過 — 依承諾不開 2020-2026 測試窗。")

print("\n[done] 第7輪（修約輪）結束，重新封槍。累積 72 組。")
