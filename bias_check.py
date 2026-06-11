# -*- coding: utf-8 -*-
"""
前視偏差 / 過擬合 驗證腳本（一次性分析用，不影響正式流程）
1. 重繪檢驗: daily_predictions_4votes.csv (2026-04-09 快照) vs strategy_signal.db (今日重算)
2. 前視量化: 票1/票2 查表 全樣本 vs 逐日expanding(point-in-time) 回測比較
3. 查表格子統計: 樣本數 / 平均 / t值
4. 交易成本影響
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# Part 0: 完整複製 strategy_signal.py 的資料管線
# ============================================================
conn_tx = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
df = pd.read_sql_query("""
    WITH RankedData AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY trading_date ORDER BY delivery_month
        ) as rn
        FROM tx_futures WHERE session = '一般'
    )
    SELECT trading_date, open_price, high_price, low_price, close_price, volume, delivery_month
    FROM RankedData WHERE rn = 1
    ORDER BY trading_date
""", conn_tx)
conn_tx.close()

conn_idx = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
tpex = pd.read_sql_query("SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn_idx)
conn_idx.close()

df['trading_date'] = pd.to_datetime(df['trading_date'])
tpex['date'] = pd.to_datetime(tpex['date'])
tpex['tpex_daily_return'] = tpex['tpex_close'].pct_change()
df = df.merge(tpex[['date', 'tpex_daily_return']], left_on='trading_date', right_on='date', how='inner').drop(columns='date')

df['daily_return_open'] = df['open_price'].pct_change()
df['daily_return_open_next_two_day'] = df['daily_return_open'].shift(-2)
df['overnight_return'] = (df['open_price'] - df['close_price'].shift(1)) / df['close_price'].shift(1) * 100
df['intraday_return'] = (df['close_price'] - df['open_price']) / df['open_price'] * 100
df['SMA_60'] = df['close_price'].rolling(60).mean()
df['price_to_SMA_60'] = df['close_price'] / df['SMA_60']
df['tpex_std_20'] = df['tpex_daily_return'].rolling(20).std()

df = df[df['trading_date'] >= '2020-01-01'].reset_index(drop=True)
df = df.dropna(subset=['tpex_daily_return']).reset_index(drop=True)

irs = df['intraday_return'].rolling(20).std()
df['s1_a'] = np.where(irs.rolling(30).mean() > irs.rolling(50).mean(), 'A1', 'A0')
df['s1_b'] = np.where(df['tpex_std_20'].rolling(20).mean() > df['tpex_std_20'].rolling(80).mean(), 'B1', 'B0')
df['s1_c'] = np.where(df['overnight_return'].rolling(40).mean() > 0, 'C1', 'C0')
# rolling 在前 window-1 筆是 NaN, np.where 仍給字串 → 需用底層 NaN 判斷 validity
s1_valid = (irs.rolling(30).mean().notna() & irs.rolling(50).mean().notna()
            & df['tpex_std_20'].rolling(20).mean().notna() & df['tpex_std_20'].rolling(80).mean().notna()
            & df['overnight_return'].rolling(40).mean().notna())

df['s2_a'] = np.where(df['intraday_return'].rolling(40).mean() > df['intraday_return'].rolling(90).mean(), 'A1', 'A0')
df['s2_b'] = np.where(df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(80).mean(), 'B1', 'B0')
df['s2_c'] = np.where(df['overnight_return'].rolling(40).mean() > df['intraday_return'].rolling(40).mean(), 'C1', 'C0')
s2_valid = (df['intraday_return'].rolling(90).mean().notna() & df['overnight_return'].rolling(80).mean().notna())

df['s3_a'] = np.where(df['intraday_return'].rolling(30).mean() > df['intraday_return'].rolling(90).mean(), 'A1', 'A0')
df['s3_b'] = np.where(df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(60).mean(), 'B1', 'B0')
df['s3_c'] = np.where(df['price_to_SMA_60'] > 1, 'C1', 'C0')
s3_valid = (df['intraday_return'].rolling(90).mean().notna() & df['overnight_return'].rolling(60).mean().notna()
            & df['price_to_SMA_60'].notna())

us = (df['high_price'] - df[['open_price', 'close_price']].max(axis=1)) / df['open_price']
us_s, us_l = us.rolling(2).mean(), us.rolling(10).mean()
ir = (df['high_price'] - df['low_price']) / df['open_price']
ir_s, ir_l = ir.rolling(5).mean(), ir.rolling(10).mean()

n = len(df)
label = df['daily_return_open_next_two_day'].values

# --- 注意: strategy_signal.py 的 notna() 檢查作用在 np.where 產生的字串欄位上，
# --- 永遠為 True（np.where 不會回傳 NaN 字串），所以正式程式的 validity mask 其實失效。
# --- 這裡為了「完全複製正式版行為」，使用與正式版相同的全 True mask。
v1v_prod = df['s1_a'].notna().values & df['s1_b'].notna().values & df['s1_c'].notna().values
v2v_prod = df['s2_a'].notna().values & df['s2_b'].notna().values & df['s2_c'].notna().values
v3v_prod = df['s3_a'].notna().values & df['s3_b'].notna().values & df['s3_c'].notna().values
print(f"[check] s1 validity mask all-True? {v1v_prod.all()} (正式版 notna 檢查是否失效)")
print(f"[check] 真正 warm-up 完成前的列數 s1={int((~s1_valid).sum())} s2={int((~s2_valid).sum())} s3={int((~s3_valid).sum())}")

# key 編碼 0-7
def encode(a, b, c):
    return ((a == 'A1').astype(int) * 4 + (b == 'B1').astype(int) * 2 + (c == 'C1').astype(int)).values

k1 = encode(df['s1_a'], df['s1_b'], df['s1_c'])
k2 = encode(df['s2_a'], df['s2_b'], df['s2_c'])

# ---- 全樣本查表（正式版做法）----
def full_sample_er(keys, valid):
    sums = np.zeros(8); cnts = np.zeros(8)
    for i in range(n):
        if valid[i] and not np.isnan(label[i]):
            sums[keys[i]] += label[i]; cnts[keys[i]] += 1
    means = np.where(cnts > 0, sums / np.maximum(cnts, 1), 0.0)
    return means[keys], sums, cnts

er1_full, sums1, cnts1 = full_sample_er(k1, v1v_prod)
er2_full, sums2, cnts2 = full_sample_er(k2, v2v_prod)
vote1_full = np.where(v1v_prod, np.where(er1_full > 0, 1, -1), 0).astype(np.int8)
vote2_full = np.where(v2v_prod, np.where(er2_full > 0, 1, -1), 0).astype(np.int8)

# ---- Point-in-time expanding 查表（無前視版）----
def pit_er(keys, valid):
    sums = np.zeros(8); cnts = np.zeros(8)
    er = np.zeros(n)
    for i in range(n):
        j = i - 2  # label[j] 需要 open[j+1], open[j+2]，在第 i 日收盤後才完全已知 (j+2<=i)
        if j >= 0 and valid[j] and not np.isnan(label[j]):
            sums[keys[j]] += label[j]; cnts[keys[j]] += 1
        if valid[i] and cnts[keys[i]] > 0:
            er[i] = sums[keys[i]] / cnts[keys[i]]
        else:
            er[i] = 0.0
    return er

er1_pit = pit_er(k1, v1v_prod)
er2_pit = pit_er(k2, v2v_prod)
vote1_pit = np.where(v1v_prod, np.where(er1_pit > 0, 1, -1), 0).astype(np.int8)
vote2_pit = np.where(v2v_prod, np.where(er2_pit > 0, 1, -1), 0).astype(np.int8)

# ---- 票3（硬編碼，依正式版）----
a3, b3, c3 = df['s3_a'].values, df['s3_b'].values, df['s3_c'].values
conds = [
    (a3 == 'A1') & (b3 == 'B1') & (c3 == 'C1'),
    (a3 == 'A1') & (b3 == 'B1') & (c3 != 'C1'),
    (a3 == 'A1') & (b3 != 'B1') & (c3 == 'C1'),
    (a3 == 'A1') & (b3 != 'B1') & (c3 != 'C1'),
    (a3 != 'A1') & (b3 == 'B1') & (c3 == 'C1'),
    (a3 != 'A1') & (b3 == 'B1') & (c3 != 'C1'),
    (a3 != 'A1') & (b3 != 'B1') & (c3 == 'C1'),
    (a3 != 'A1') & (b3 != 'B1') & (c3 != 'C1'),
]
vals = [0.001157, -0.001072, 0.000314, -0.004241, 0.001899, 0.001945, -0.001060, 0.002373]
er3 = np.select(conds, vals, default=0.0)
vote3 = np.where(v3v_prod, np.where(er3 > 0, 1, -1), 0).astype(np.int8)

# ---- 票4 ----
us_up = (us_s > us_l).values & us_s.notna().values & us_l.notna().values
ir_up = (ir_s > ir_l).values & ir_s.notna().values & ir_l.notna().values
ts = us_up.astype(np.int8) + ir_up.astype(np.int8)
vote4 = np.where(ts == 2, 1, np.where(ts == 0, -1, 0)).astype(np.int8)

# ---- 結算日 ----
dm = df['delivery_month'].values
is_settlement = np.zeros(n, dtype=bool)
is_settlement[:-1] = dm[:-1] != dm[1:]

open_arr = df['open_price'].values.astype(np.float64)
close_arr = df['close_price'].values.astype(np.float64)


def backtest(v1, v2):
    total = (v1 + v2 + vote3 + vote4).astype(np.int8)
    target = np.where(total >= 2, 1, np.where(total <= -2, -1, 0)).astype(np.int8)
    positions = np.zeros(n, dtype=np.int8)
    daily_pnls = np.zeros(n, dtype=np.float64)
    pos = 0; ep = np.nan
    for i in range(1, n):
        o, c = open_arr[i], close_arr[i]
        pnl = 0.0
        if is_settlement[i]:
            if pos == 1: pnl = o - ep
            elif pos == -1: pnl = ep - o
            pos = 0; ep = np.nan
        else:
            tp = target[i - 1]
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
        daily_pnls[i] = pnl
    return target, positions, daily_pnls


def perf(daily_pnls, positions, name):
    cum = daily_pnls.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min()
    sharpe = daily_pnls.mean() / daily_pnls.std() * np.sqrt(250) if daily_pnls.std() > 0 else 0
    sides = np.abs(np.diff(positions.astype(int), prepend=0)).sum()
    yearly = pd.Series(daily_pnls, index=df['trading_date']).groupby(df['trading_date'].dt.year.values).sum()
    print(f"\n=== {name} ===")
    print(f"  總損益: {daily_pnls.sum():>10,.0f} 點   MDD: {mdd:>9,.0f}   Sharpe: {sharpe:5.2f}   交易邊數: {sides}")
    print("  年度: " + "  ".join(f"{y}:{v:+,.0f}" for y, v in yearly.items()))
    return daily_pnls.sum(), mdd, sharpe, sides


print("\n" + "=" * 70)
print("Part 1: 全樣本查表（正式版 = 含前視） vs Point-in-time（無前視）")
print("=" * 70)
tg_f, pos_f, pnl_f = backtest(vote1_full, vote2_full)
total_f, mdd_f, sh_f, sides_f = perf(pnl_f, pos_f, "全樣本查表（正式版，含前視）")

# 驗證複製是否與 DB 一致
conn = sqlite3.connect(os.path.join(BASE, 'strategy_signal.db'))
db_sig = pd.read_sql_query("SELECT trading_date, vote1, vote2, vote3, vote4, total_votes, target_position, position, daily_pnl, cumulative_pnl FROM daily_signals ORDER BY trading_date", conn)
conn.close()
print(f"\n[replication check] DB 最終累積損益 = {db_sig['cumulative_pnl'].iloc[-1]:,.1f}，本腳本 = {pnl_f.cumsum()[-1]:,.1f}")
match_v1 = (db_sig['vote1'].values == vote1_full).mean() * 100
match_v2 = (db_sig['vote2'].values == vote2_full).mean() * 100
print(f"[replication check] vote1 重合率 {match_v1:.1f}%  vote2 重合率 {match_v2:.1f}% (應為 100%)")

tg_p, pos_p, pnl_p = backtest(vote1_pit, vote2_pit)
total_p, mdd_p, sh_p, sides_p = perf(pnl_p, pos_p, "Point-in-time 查表（無前視）")

# 2021 之後（給 expanding 一年暖身）的對比
m21 = (df['trading_date'] >= '2021-01-01').values
print(f"\n  [2021+ 對比] 全樣本: {pnl_f[m21].sum():>10,.0f} 點   PIT: {pnl_p[m21].sum():>10,.0f} 點")

flip1 = (vote1_full != vote1_pit).mean() * 100
flip2 = (vote2_full != vote2_pit).mean() * 100
tgt_diff = (tg_f != tg_p).mean() * 100
print(f"\n  票1 全樣本vs PIT 不一致天數比例: {flip1:.1f}%")
print(f"  票2 全樣本vs PIT 不一致天數比例: {flip2:.1f}%")
print(f"  目標部位不一致比例: {tgt_diff:.1f}%")

print("\n" + "=" * 70)
print("Part 2: 查表格子統計（全樣本 2020+）— 平均值是否顯著異於 0")
print("=" * 70)
lab_std = np.nanstd(label)
print(f"label(兩日後開盤報酬) 全樣本 std = {lab_std:.4%}")
for nm, sums, cnts in [("票1", sums1, cnts1), ("票2", sums2, cnts2)]:
    print(f"\n  {nm} 8 格:")
    print(f"  {'格':>4} {'樣本數':>6} {'平均':>10} {'SE':>10} {'|t|':>6}")
    for g in range(8):
        if cnts[g] > 0:
            m = sums[g] / cnts[g]
            se = lab_std / np.sqrt(cnts[g])
            print(f"  {g:>4} {int(cnts[g]):>6} {m:>10.5f} {se:>10.5f} {abs(m/se):>6.2f}")

# 票3: 目前樣本的條件平均 vs 硬編碼值（符號是否一致）
print(f"\n  票3 硬編碼 vs 目前樣本(2020+)實際條件平均:")
print(f"  {'格':>4} {'樣本數':>6} {'硬編碼':>10} {'目前樣本':>10} {'符號一致':>8} {'|t|':>6}")
for g, cond in enumerate(conds):
    mask = cond & v3v_prod & ~np.isnan(label)
    cnt = mask.sum()
    if cnt > 0:
        m = label[mask].mean()
        se = lab_std / np.sqrt(cnt)
        same = "O" if np.sign(m) == np.sign(vals[g]) else "X"
        print(f"  {g:>4} {cnt:>6} {vals[g]:>10.6f} {m:>10.6f} {same:>8} {abs(m/se):>6.2f}")

print("\n" + "=" * 70)
print("Part 3: 重繪檢驗 — 2026-04-09 CSV 快照 vs 今日 DB（同一歷史日期）")
print("=" * 70)
csv = pd.read_csv(os.path.join(BASE, 'daily_predictions_4votes.csv'))
csv['trading_date'] = pd.to_datetime(csv['trading_date']).dt.strftime('%Y-%m-%d')
merged = csv.merge(db_sig, on='trading_date', suffixes=('_csv', '_db'))
print(f"  重疊日期數: {len(merged)} ({merged['trading_date'].min()} ~ {merged['trading_date'].max()})")
for v_csv, v_db, nm in [('vote1_csv', 'vote1_db', '票1'), ('vote2_csv', 'vote2_db', '票2'),
                         ('vote3_csv', 'vote3_db', '票3'), ('vote4_tech', 'vote4', '票4')]:
    diff = (merged[v_csv] != merged[v_db]).sum()
    print(f"  {nm}: 歷史日期投票被改寫 {diff} 天 ({diff/len(merged)*100:.1f}%)")
tp_diff = (merged['target_position_csv'] != merged['target_position_db']).sum()
print(f"  目標部位被改寫: {tp_diff} 天 ({tp_diff/len(merged)*100:.1f}%)")
if tp_diff > 0:
    changed = merged[merged['target_position_csv'] != merged['target_position_db']]
    print("  被改寫的日期(前15筆): " + ", ".join(changed['trading_date'].head(15)))

print("\n" + "=" * 70)
print("Part 4: 交易成本影響（正式版回測, 全期間）")
print("=" * 70)
for cost_per_side in [1.5, 3.0]:
    net = pnl_f.sum() - sides_f * cost_per_side
    print(f"  每邊成本 {cost_per_side} 點（稅+手續費+滑價）: 毛利 {pnl_f.sum():,.0f} - 成本 {sides_f * cost_per_side:,.0f} = 淨利 {net:,.0f} 點")
    net_p = pnl_p.sum() - np.abs(np.diff(pos_p.astype(int), prepend=0)).sum() * cost_per_side
    print(f"    (PIT 版淨利: {net_p:,.0f} 點)")

print("\n[done]")
