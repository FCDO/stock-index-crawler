# -*- coding: utf-8 -*-
"""
因子研究 第8輪：引擎 A（市場狀態 32 模式）PIT 查表 — 封槍後第 2 次修約

═══ 修約記錄 ═══
R7 後已重新封槍。本輪為使用者於 2026-06-12 明示授權之修約 #2，
範圍嚴格限定 1 組設定（零自由參數），跑完即重新封槍。累積測試 72 → 73。

═══ 事前承諾（運行前寫定；先驗已向使用者完整揭露）═══

先驗（不利）：本方法類別（條件平均查表）為 v1 過擬合根源、R1 起明文禁用；
其誠實 PIT 版（v2 票1-3，8 狀態）dev Sharpe ≈ 0；五個維度的單因子皆已測過
（SMA200=0.50、LT=0.54、TSMOM60=0.15、波動管理失敗）。本測試唯一增量 = 交互作用。

設定（唯一一組，無任何可調旋鈕）：
- 狀態：market_analogue.py 的 5 核心維度 → 32 模式，定義原樣凍結照搬
  （SMA200 / 60日動能 / 20日波動 vs 252日中位 / 距252日高點5% / LT淨部位 vs 20日均）
- 查表：PIT expanding。標籤 = open(j+1)→open(j+2) 報酬（滯後 2 天可得，與 v2 票1-3 同）
  位置規則：cell 均值 > 0 → 多、< 0 → 空、cell 無樣本 → 空手
  （註：v2 票的空 cell 行為是 -1，屬歷史 quirk；本輪改空手，較保守，事前宣告）
- 執行：T 收盤算出 → T+1 開盤執行；結算日開盤強平；成本 1.5 點/邊（全程與既有研究一致）
- 表自 2004-08（資料起點）累積；早期 cell 稀疏屬誠實現實的一部分

判定（寫死）：
- dev 2005-2019（15年）、半段 2005-2012 / 2013-2019
- 門檻（R7 加嚴版）：dev Sharpe >= 0.8 且 正年 >= 10/15 且 兩半段 Sharpe >= 0.3
- dev 未過 → 不開 2020-2026 測試窗
- dev 通過 → test 一次性，僅作否決閘（testSh < 0.3 否決；>= 0.3 進 signal_ledger
  紙上追蹤 12 個月，無論如何不直接部署）
- 另列 32 cell 的 dev 期末佔用診斷（樣本數，不含報酬資訊）供解讀噪音

本輪結束後重新封槍。
"""
import math
import sqlite3
import os

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5

# ================= 資料 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price, close_price, delivery_month FROM R WHERE rn=1
    ORDER BY trading_date""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
lt = pd.read_sql_query(
    "SELECT date, t5_spec_long - t5_spec_short AS lt_net FROM large_trader_tx ORDER BY date", conn)
conn.close()

df['trading_date'] = pd.to_datetime(df['trading_date'])
lt['date'] = pd.to_datetime(lt['date'])
df = df.merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date')
df['lt_net'] = df['lt_net'].ffill(limit=5)

n = len(df)
yr = df['trading_date'].dt.year.values
o = df['open_price'].values.astype(float)
c = df['close_price'].values.astype(float)
dm = df['delivery_month'].values

roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]
adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)
pct = np.zeros(n)
pct[1:] = adj_ret[1:] / c[:-1]


def _roll(x, w, fn):
    return getattr(pd.Series(x).rolling(w), fn)().values


# ================= 5 維 → 32 模式（與 market_analogue.py 完全相同，凍結照搬）=================
sma200 = _roll(adjC, 200, 'mean')
mom60 = adjC - np.concatenate([np.full(60, np.nan), adjC[:-60]])
vol20 = _roll(pct, 20, 'std') * math.sqrt(252)
volmed = _roll(vol20, 252, 'median')
hi252 = _roll(adjC, 252, 'max')
dd = (adjC - hi252) / c
ltv = df['lt_net'].values.astype(float)
ltma = _roll(ltv, 20, 'mean')

bits = np.stack([
    (adjC > sma200), (mom60 > 0), (vol20 > volmed), (dd > -0.05), (ltv > ltma),
]).astype(int)
valid = ~(np.isnan(sma200) | np.isnan(mom60) | np.isnan(vol20)
          | np.isnan(volmed) | np.isnan(dd) | np.isnan(ltv) | np.isnan(ltma))
pid = np.where(valid, (bits[0] * 16 + bits[1] * 8 + bits[2] * 4 + bits[3] * 2 + bits[4]), -1)

# ================= PIT expanding 查表（標籤滯後 2 天；空 cell → 空手）=================
label = pd.Series(o).pct_change().shift(-2).values   # label[j] = open(j+1)->open(j+2)

sums = np.zeros(32)
cnts = np.zeros(32)
sig_next = np.zeros(n)                # 在 i 日收盤算出的「明日應持」
for i in range(n):
    j = i - 2
    if j >= 0 and pid[j] >= 0 and not np.isnan(label[j]):
        sums[pid[j]] += label[j]
        cnts[pid[j]] += 1
    if pid[i] >= 0 and cnts[pid[i]] > 0:
        er = sums[pid[i]] / cnts[pid[i]]
        sig_next[i] = 1 if er > 0 else -1

exec_sig = np.zeros(n)
exec_sig[1:] = sig_next[:-1]          # T+1 開盤執行

# dev 期末 cell 佔用診斷（只看樣本數）
dev_end = np.argmax(yr > 2019) if (yr > 2019).any() else n
cnts_dev = np.zeros(32)
for i in range(dev_end):
    j = i - 2
    if j >= 0 and pid[j] >= 0 and not np.isnan(label[j]):
        cnts_dev[pid[j]] += 1


# ================= 引擎（與第1-7輪相同）=================
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
    return dict(tot=x.sum(), sharpe=sh, t=tt, mdd=mdd,
                posy=int((ys > 0).sum()), ny=len(ys))


DEV = (yr >= 2005) & (yr <= 2019)
H1 = (yr >= 2005) & (yr <= 2012)
H2 = (yr >= 2013) & (yr <= 2019)
TEST = (yr >= 2020)

pnl, sides = run_cont(exec_sig)
pnl_long, _ = run_cont(np.ones(n))

d = stats(pnl, DEV)
h1 = stats(pnl, H1)
h2 = stats(pnl, H2)
bl = stats(pnl_long, DEV)

print("=" * 100)
print("第8輪（修約#2）引擎A 32模式 PIT 查表   dev 2005-2019")
print(f"基準[永遠做多] dev: 淨 {bl['tot']:,.0f}  Sharpe {bl['sharpe']:.2f}  正年 {bl['posy']}/15")
print("=" * 100)
print(f"{'設定':<22} {'淨損益':>9} {'Sharpe':>7} {'t值':>6} {'正年':>6} {'半1Sh':>6} {'半2Sh':>6} {'MDD':>8} {'邊數':>6}")
print("-" * 100)
print(f"{'ENGINE_A_PIT(32)':<22} {d['tot']:>9,.0f} {d['sharpe']:>7.2f} {d['t']:>6.2f} "
      f"{d['posy']:>4}/15 {h1['sharpe']:>6.2f} {h2['sharpe']:>6.2f} {d['mdd']:>8,.0f} {sides:>6}")

occ = np.sort(cnts_dev)[::-1]
print(f"\nCell 佔用診斷（dev 期末）：>=100 樣本 {int((cnts_dev>=100).sum())}/32 格、"
      f">=30 樣本 {int((cnts_dev>=30).sum())}/32 格、中位 {np.median(cnts_dev):,.0f}、最大 {occ[0]:,.0f}")

print()
print("=" * 100)
print("判定（寫死）: dev Sh>=0.8 且 正年>=10/15 且 兩半段 Sh>=0.3")
print("=" * 100)
ok = (d['sharpe'] >= 0.8 and d['posy'] >= 10 and h1['sharpe'] >= 0.3 and h2['sharpe'] >= 0.3)
if ok:
    t = stats(pnl, TEST)
    verdict = '否決' if t['sharpe'] < 0.3 else '進入 ledger 紙上追蹤（不部署）'
    print(f"[PASS] dev 通過 → 一次性 test 2020-2026（僅否決閘）:")
    print(f"       test 淨 {t['tot']:,.0f}  Sh {t['sharpe']:.2f}  正年 {t['posy']}/7  MDD {t['mdd']:,.0f}  -> {verdict}")
else:
    print(f"[FAIL] Sh={d['sharpe']:.2f}(需>=0.8)  正年={d['posy']}/15(需>=10)  "
          f"半段={h1['sharpe']:.2f}/{h2['sharpe']:.2f}(需>=0.3) — 依承諾不開測試窗")

print("\n[done] 第8輪（修約#2）結束，重新封槍。累積 73 組。")
