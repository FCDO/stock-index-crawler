# -*- coding: utf-8 -*-
"""
決定性測試: 2000-2019 holdout
把「現行凍結規則」(票3硬編碼 + 以2020+資料建的票1/2查表 + 票4 + 閾值±2)
套用到從未參與任何擬合的 2000-2019 資料上。
若策略真有 alpha，OOS Sharpe 應與 in-sample 同量級；若是過擬合，OOS 會崩。
同時輸出: 部署後真實 OOS (2026-04-09~) 策略 vs 永遠做多 / 2020 暖身期 PnL。
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))

conn_tx = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH RankedData AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) as rn
        FROM tx_futures WHERE session = '一般'
    )
    SELECT trading_date, open_price, high_price, low_price, close_price, volume, delivery_month
    FROM RankedData WHERE rn = 1 ORDER BY trading_date
""", conn_tx)
conn_tx.close()

conn_idx = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tpex = pd.read_sql_query("SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn_idx)
conn_idx.close()
print(f"tpex 資料範圍: {tpex['date'].min()} ~ {tpex['date'].max()}")

df['trading_date'] = pd.to_datetime(df['trading_date'])
tpex['date'] = pd.to_datetime(tpex['date'])
tpex['tpex_daily_return'] = tpex['tpex_close'].pct_change()
df = df.merge(tpex[['date', 'tpex_daily_return']], left_on='trading_date', right_on='date', how='inner').drop(columns='date')
df = df.dropna(subset=['tpex_daily_return']).reset_index(drop=True)

df['daily_return_open'] = df['open_price'].pct_change()
df['label'] = df['daily_return_open'].shift(-2)
df['overnight_return'] = (df['open_price'] - df['close_price'].shift(1)) / df['close_price'].shift(1) * 100
df['intraday_return'] = (df['close_price'] - df['open_price']) / df['open_price'] * 100
df['SMA_60'] = df['close_price'].rolling(60).mean()
df['price_to_SMA_60'] = df['close_price'] / df['SMA_60']
df['tpex_std_20'] = df['tpex_daily_return'].rolling(20).std()

# 因子: 在全 2000+ 序列上計算（warm-up 在 2000 年消化，2001+ 全部乾淨）
irs = df['intraday_return'].rolling(20).std()
f1a = (irs.rolling(30).mean() > irs.rolling(50).mean())
f1b = (df['tpex_std_20'].rolling(20).mean() > df['tpex_std_20'].rolling(80).mean())
f1c = (df['overnight_return'].rolling(40).mean() > 0)
v1ok = irs.rolling(50).mean().notna() & df['tpex_std_20'].rolling(80).mean().notna() & df['overnight_return'].rolling(40).mean().notna()

f2a = (df['intraday_return'].rolling(40).mean() > df['intraday_return'].rolling(90).mean())
f2b = (df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(80).mean())
f2c = (df['overnight_return'].rolling(40).mean() > df['intraday_return'].rolling(40).mean())
v2ok = df['intraday_return'].rolling(90).mean().notna() & df['overnight_return'].rolling(80).mean().notna()

f3a = (df['intraday_return'].rolling(30).mean() > df['intraday_return'].rolling(90).mean())
f3b = (df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(60).mean())
f3c = (df['price_to_SMA_60'] > 1)
v3ok = df['intraday_return'].rolling(90).mean().notna() & df['overnight_return'].rolling(60).mean().notna() & df['price_to_SMA_60'].notna()

us = (df['high_price'] - df[['open_price', 'close_price']].max(axis=1)) / df['open_price']
us_s, us_l = us.rolling(2).mean(), us.rolling(10).mean()
ir = (df['high_price'] - df['low_price']) / df['open_price']
ir_s, ir_l = ir.rolling(5).mean(), ir.rolling(10).mean()

k1 = (f1a.astype(int) * 4 + f1b.astype(int) * 2 + f1c.astype(int)).values
k2 = (f2a.astype(int) * 4 + f2b.astype(int) * 2 + f2c.astype(int)).values
label = df['label'].values
n = len(df)
dates = df['trading_date']

# ---- 查表「凍結於 2020+ 樣本」(複製正式版的表, 用其 2020+ 期間自己的因子定義) ----
# 注意: 正式版因子是 2020 後重新起算(含 warm-up bug)。為重現其表，這裡單獨重算 2020+ 子樣本的 key。
sub = df[df['trading_date'] >= '2020-01-01'].reset_index(drop=True)
irs_s = sub['intraday_return'].rolling(20).std()
g1a = (irs_s.rolling(30).mean() > irs_s.rolling(50).mean())
g1b = (sub['tpex_std_20'].rolling(20).mean() > sub['tpex_std_20'].rolling(80).mean())
g1c = (sub['overnight_return'].rolling(40).mean() > 0)
g2a = (sub['intraday_return'].rolling(40).mean() > sub['intraday_return'].rolling(90).mean())
g2b = (sub['overnight_return'].rolling(40).mean() > sub['overnight_return'].rolling(80).mean())
g2c = (sub['overnight_return'].rolling(40).mean() > sub['intraday_return'].rolling(40).mean())
sk1 = (g1a.astype(int) * 4 + g1b.astype(int) * 2 + g1c.astype(int)).values
sk2 = (g2a.astype(int) * 4 + g2b.astype(int) * 2 + g2c.astype(int)).values
slab = sub['label'].values

def table(keys, lab):
    sums = np.zeros(8); cnts = np.zeros(8)
    for i in range(len(keys)):
        if not np.isnan(lab[i]):
            sums[keys[i]] += lab[i]; cnts[keys[i]] += 1
    return np.where(cnts > 0, sums / np.maximum(cnts, 1), 0.0)

T1 = table(sk1, slab)   # 正式版 gr1 (2020+ 全樣本)
T2 = table(sk2, slab)

er1 = T1[k1]
er2 = T2[k2]
vote1 = np.where(v1ok, np.where(er1 > 0, 1, -1), 0).astype(np.int8)
vote2 = np.where(v2ok, np.where(er2 > 0, 1, -1), 0).astype(np.int8)

VALS3 = np.array([0.001157, -0.001072, 0.000314, -0.004241, 0.001899, 0.001945, -0.001060, 0.002373])
# 正式版 np.select 條件順序: (a,b,c)=(1,1,1) 是第 0 個 → index = 7 - (a*4+b*2+c)
idx3 = (f3a.astype(int) * 4 + f3b.astype(int) * 2 + f3c.astype(int)).values
er3 = VALS3[7 - idx3]
vote3 = np.where(v3ok, np.where(er3 > 0, 1, -1), 0).astype(np.int8)

us_up = (us_s > us_l).values & us_l.notna().values
ir_up = (ir_s > ir_l).values & ir_l.notna().values
ts = us_up.astype(np.int8) + ir_up.astype(np.int8)
vote4 = np.where(ts == 2, 1, np.where(ts == 0, -1, 0)).astype(np.int8)

total = (vote1 + vote2 + vote3 + vote4).astype(np.int8)
target = np.where(total >= 2, 1, np.where(total <= -2, -1, 0)).astype(np.int8)

dm = df['delivery_month'].values
is_settle = np.zeros(n, dtype=bool)
is_settle[:-1] = dm[:-1] != dm[1:]
o_arr = df['open_price'].values.astype(float)
c_arr = df['close_price'].values.astype(float)

def run_bt(tgt):
    positions = np.zeros(n, dtype=np.int8)
    pnls = np.zeros(n)
    pos = 0; ep = np.nan
    for i in range(1, n):
        o, c = o_arr[i], c_arr[i]
        pnl = 0.0
        if is_settle[i]:
            if pos == 1: pnl = o - ep
            elif pos == -1: pnl = ep - o
            pos = 0; ep = np.nan
        else:
            tp = tgt[i - 1]
            if tp == 1:
                if pos == -1: pnl += ep - o; ep = o
                elif pos == 0: ep = o
                pos = 1; pnl += c - ep; ep = c
            elif tp == -1:
                if pos == 1: pnl += o - ep; ep = o
                elif pos == 0: ep = o
                pos = -1; pnl += ep - c; ep = c
            else:
                if pos == 1: pnl += o - ep
                elif pos == -1: pnl += ep - o
                pos = 0; ep = np.nan
        positions[i] = pos
        pnls[i] = pnl
    return positions, pnls

pos_s, pnl_s = run_bt(target)
pos_l, pnl_l = run_bt(np.ones(n, dtype=np.int8))   # 永遠做多

def stats(p, mask, name):
    x = p[mask]
    sh = x.mean() / x.std() * np.sqrt(250) if x.std() > 0 else 0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min()
    return f"{name}: 總損益 {x.sum():>9,.0f}  Sharpe {sh:5.2f}  MDD {mdd:>9,.0f}"

print("\n" + "=" * 76)
print("OOS holdout: 2001-2019（規則凍結, 全部以現行 2020+ 擬合的表/常數投票）")
print("=" * 76)
yr = dates.dt.year.values
oos = (yr >= 2001) & (yr <= 2019)
ins = (yr >= 2020)
print("  " + stats(pnl_s, oos, "策略   (OOS 2001-2019)"))
print("  " + stats(pnl_l, oos, "永遠做多(OOS 2001-2019)"))
print("  " + stats(pnl_s, ins, "策略   (in-sample 2020+)"))
print("  " + stats(pnl_l, ins, "永遠做多(in-sample 2020+)"))

print(f"\n  {'年':>5} {'策略':>9} {'永遠做多':>9}   |  {'年':>5} {'策略':>9} {'永遠做多':>9}")
ys = pd.Series(pnl_s).groupby(yr).sum()
yb = pd.Series(pnl_l).groupby(yr).sum()
years_list = [y for y in ys.index if 2001 <= y <= 2019]
half = (len(years_list) + 1) // 2
for i in range(half):
    y1 = years_list[i]
    line = f"  {y1:>5} {ys[y1]:>+9,.0f} {yb[y1]:>+9,.0f}"
    if i + half < len(years_list):
        y2 = years_list[i + half]
        line += f"   |  {y2:>5} {ys[y2]:>+9,.0f} {yb[y2]:>+9,.0f}"
    print(line)

# 勝負年統計
w_oos = sum(1 for y in years_list if ys[y] > yb[y])
print(f"\n  OOS 19 年中策略贏過永遠做多的年數: {w_oos}/19")
pos_years = sum(1 for y in years_list if ys[y] > 0)
print(f"  OOS 19 年中策略為正報酬的年數: {pos_years}/19")

# ---- 部署後真實 OOS ----
print("\n" + "=" * 76)
print("部署後真實 OOS（2026-04-09 ~ 2026-06-11）")
print("=" * 76)
m_oos2 = (dates >= '2026-04-09').values
print("  " + stats(pnl_s, m_oos2, "策略    "))
print("  " + stats(pnl_l, m_oos2, "永遠做多"))

# ---- 2020 暖身期間 (正式版 warm-up bug 影響範圍) 的 PnL ----
conn = sqlite3.connect(os.path.join(BASE, 'strategy_signal.db'))
db = pd.read_sql_query("SELECT trading_date, daily_pnl FROM daily_signals ORDER BY trading_date LIMIT 90", conn)
conn.close()
print(f"\n  正式版 2020 前 90 個交易日(因子窗口未滿就投票)的 PnL 合計: {db['daily_pnl'].sum():+,.0f} 點 ({db['trading_date'].iloc[0]} ~ {db['trading_date'].iloc[-1]})")
print("\n[done]")
