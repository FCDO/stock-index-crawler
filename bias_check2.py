# -*- coding: utf-8 -*-
"""補充驗證: 永遠做多基準 / 多空貢獻拆解 / 成本影響 / CSV與DB的PnL差異(結算日處理)"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))

conn = sqlite3.connect(os.path.join(BASE, 'strategy_signal.db'))
sig = pd.read_sql_query("SELECT * FROM daily_signals ORDER BY trading_date", conn)
conn.close()

conn_tx = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH RankedData AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) as rn
        FROM tx_futures WHERE session = '一般'
    )
    SELECT trading_date, open_price, close_price, delivery_month
    FROM RankedData WHERE rn = 1 ORDER BY trading_date
""", conn_tx)
conn_tx.close()
df['trading_date'] = pd.to_datetime(df['trading_date']).dt.strftime('%Y-%m-%d')
df = df[df['trading_date'].isin(sig['trading_date'])].reset_index(drop=True)
assert len(df) == len(sig)

n = len(df)
dm = df['delivery_month'].values
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]
o = df['open_price'].values.astype(float)
c = df['close_price'].values.astype(float)

# ---- 永遠做多基準（同樣結算日開盤平倉、次日開盤重進）----
pnl_long = np.zeros(n)
pos = 0; ep = np.nan
for i in range(1, n):
    if is_settle[i]:
        pnl_long[i] = (o[i] - ep) if pos == 1 else 0.0
        pos = 0; ep = np.nan
    else:
        if pos == 0:
            ep = o[i]
        pos = 1
        pnl_long[i] = c[i] - ep
        ep = c[i]

years = pd.to_datetime(sig['trading_date']).dt.year.values
strat = sig['daily_pnl'].values
print("=" * 72)
print("A. 策略 vs 永遠做多（相同結算日處理, 2020-01 ~ 2026-06）")
print("=" * 72)
for name, p in [("四票制策略", strat), ("永遠做多", pnl_long)]:
    cum = p.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min()
    sh = p.mean() / p.std() * np.sqrt(250)
    print(f"  {name}: 總損益 {p.sum():>10,.0f}  MDD {mdd:>10,.0f}  Sharpe {sh:5.2f}")
yl = pd.Series(strat).groupby(years).sum()
yb = pd.Series(pnl_long).groupby(years).sum()
print(f"\n  {'年':>6} {'策略':>10} {'永遠做多':>10}")
for y in yl.index:
    print(f"  {y:>6} {yl[y]:>+10,.0f} {yb[y]:>+10,.0f}")

# ---- 多空貢獻 ----
pos_arr = sig['position'].values
print(f"\n  持倉分布: 做多 {(pos_arr==1).sum()} 天 ({(pos_arr==1).mean()*100:.0f}%) / "
      f"做空 {(pos_arr==-1).sum()} 天 ({(pos_arr==-1).mean()*100:.0f}%) / 空手 {(pos_arr==0).sum()} 天")
print(f"  做多日損益合計: {strat[pos_arr==1].sum():+,.0f}  做空日損益合計: {strat[pos_arr==-1].sum():+,.0f}")
print(f"  (做空日中市場下跌日比例: {((c-o)[pos_arr==-1] < 0).mean()*100:.0f}%)")

# ---- 成本 ----
sides = np.abs(np.diff(pos_arr.astype(int), prepend=0)).sum()
# 結算日平倉也算一邊: position 由 1/-1 → 0 已含在 diff 中
print("\n" + "=" * 72)
print("B. 交易成本影響")
print("=" * 72)
print(f"  交易邊數(進出合計): {sides}")
for cost in [1.5, 3.0]:
    print(f"  每邊 {cost} 點: 毛利 {strat.sum():,.0f} → 淨利 {strat.sum() - sides*cost:,.0f} 點 (侵蝕 {sides*cost/strat.sum()*100:.1f}%)")

# ---- CSV(notebook,無結算處理) vs DB(正式,有結算處理) 的 PnL 差異 ----
print("\n" + "=" * 72)
print("C. Notebook(無結算日處理) vs 正式版(有) 的回測差異 — 重疊期間")
print("=" * 72)
csv = pd.read_csv(os.path.join(BASE, 'daily_predictions_4votes.csv'))
csv['trading_date'] = pd.to_datetime(csv['trading_date']).dt.strftime('%Y-%m-%d')
m = csv.merge(sig[['trading_date', 'daily_pnl', 'position']], on='trading_date', suffixes=('_nb', '_db'))
diff_days = (np.abs(m['daily_pnl_nb'] - m['daily_pnl_db']) > 1e-6).sum()
print(f"  重疊 {len(m)} 天中 daily_pnl 不同的天數: {diff_days}")
print(f"  重疊期間總損益: notebook {m['daily_pnl_nb'].sum():,.0f} 點 vs 正式版 {m['daily_pnl_db'].sum():,.0f} 點 (差 {m['daily_pnl_nb'].sum()-m['daily_pnl_db'].sum():+,.0f})")
print("\n[done]")
