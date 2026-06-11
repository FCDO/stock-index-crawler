# -*- coding: utf-8 -*-
"""
台指期策略訊號 v2 - 誠實版（與 v1 並行，不影響既有 daily_signals 表）

相對 v1（strategy_signal.py）的修正：
1. 票1/票2/票3 查表改為 point-in-time expanding：每一天只用「當天收盤前已知」
   的歷史標籤（標籤滯後 2 天），且自 2001 年起累積 — 回測不再偷看未來、
   歷史投票永不重繪。票3 不再使用 2020+ 樣本擬合的硬編碼常數。
2. 修正暖身期 bug：v1 的 notna() 檢查作用在 np.where 字串上永遠為 True；
   v2 因子在 2000 年起的全歷史上計算並以底層 rolling 是否完成判斷有效性。
3. 趨勢欄位：roll-adjusted 連續收盤 vs SMA200 的多/空手訊號（trend_position）。
   這是 2001-2019 與 2020-2026 兩段期間唯一行為一致的訊號（風控用，非 alpha）。
4. 兩套訊號（四票制 PIT / SMA200 趨勢）同場回測（結算日開盤強平，與 v1 相同），
   存入 strategy_signal.db 的 daily_signals_v2 表，供並行追蹤比較。

注意：v2 的歷史投票與 v1 不同是「預期中的」— v1 的歷史投票含前視。
最新一天的四票訊號兩版理論上接近，但票3改 PIT 後可能不同。
"""
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TX_DB = os.path.join(BASE_DIR, 'tx_futures.db')
INDEX_DB = os.path.join(BASE_DIR, 'stock_index.db')
INST_DB = os.path.join(BASE_DIR, 'institutional.db')
SIGNAL_DB = os.path.join(BASE_DIR, 'strategy_signal.db')

OUTPUT_START = '2020-01-01'   # 輸出起始日（與 v1 對齊，方便對照）
SMA_N = 200                   # 趨勢窗口
LT_N = 20                     # 大額交易人特定法人淨部位 vs N日均（紙上追蹤艙）


def _shift1(x):
    out = np.empty(len(x))
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


def compute_signals_v2():
    # ---- 載入（全歷史，因子自 2000 年起暖身）----
    conn_tx = sqlite3.connect(TX_DB)
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

    conn_idx = sqlite3.connect(INDEX_DB)
    tpex = pd.read_sql_query(
        "SELECT date, close AS tpex_close FROM tpex_index ORDER BY date", conn_idx
    )
    conn_idx.close()

    conn_inst = sqlite3.connect(INST_DB)
    lt = pd.read_sql_query(
        "SELECT date, t5_spec_long, t5_spec_short FROM large_trader_tx ORDER BY date", conn_inst
    )
    conn_inst.close()

    df['trading_date'] = pd.to_datetime(df['trading_date'])
    tpex['date'] = pd.to_datetime(tpex['date'])
    lt['date'] = pd.to_datetime(lt['date'])
    tpex['tpex_daily_return'] = tpex['tpex_close'].pct_change()
    df = df.merge(
        tpex[['date', 'tpex_daily_return']],
        left_on='trading_date', right_on='date', how='inner'
    ).drop(columns='date')
    df = df.merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date')
    df = df.reset_index(drop=True)

    n = len(df)
    o = df['open_price'].values.astype(float)
    h = df['high_price'].values.astype(float)
    l = df['low_price'].values.astype(float)
    c = df['close_price'].values.astype(float)
    dm = df['delivery_month'].values

    # ---- 報酬與標籤 ----
    df['daily_return_open'] = df['open_price'].pct_change()
    label = df['daily_return_open'].shift(-2).values   # 未來兩日開盤報酬（僅供 PIT 累積）
    df['overnight_return'] = (df['open_price'] - df['close_price'].shift(1)) / df['close_price'].shift(1) * 100
    df['intraday_return'] = (df['close_price'] - df['open_price']) / df['open_price'] * 100
    df['tpex_std_20'] = df['tpex_daily_return'].rolling(20).std()
    df['SMA_60'] = df['close_price'].rolling(60).mean()
    df['price_to_SMA_60'] = df['close_price'] / df['SMA_60']

    # ---- roll-adjusted 連續序列（趨勢訊號用，去除換月基差跳動）----
    roll_first = np.zeros(n, dtype=bool)
    roll_first[1:] = dm[1:] != dm[:-1]
    adj_ret = np.zeros(n)
    adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
    adjC = c[0] + np.cumsum(adj_ret)
    sma_trend = pd.Series(adjC).rolling(SMA_N).mean().values

    # ---- 因子（全歷史計算 + 真正的有效性判斷）----
    irs = df['intraday_return'].rolling(20).std()
    s1a = (irs.rolling(30).mean() > irs.rolling(50).mean()).values
    s1b = (df['tpex_std_20'].rolling(20).mean() > df['tpex_std_20'].rolling(80).mean()).values
    s1c = (df['overnight_return'].rolling(40).mean() > 0).values
    v1ok = (irs.rolling(50).mean().notna() & df['tpex_std_20'].rolling(80).mean().notna()
            & df['overnight_return'].rolling(40).mean().notna()).values

    s2a = (df['intraday_return'].rolling(40).mean() > df['intraday_return'].rolling(90).mean()).values
    s2b = (df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(80).mean()).values
    s2c = (df['overnight_return'].rolling(40).mean() > df['intraday_return'].rolling(40).mean()).values
    v2ok = (df['intraday_return'].rolling(90).mean().notna()
            & df['overnight_return'].rolling(80).mean().notna()).values

    s3a = (df['intraday_return'].rolling(30).mean() > df['intraday_return'].rolling(90).mean()).values
    s3b = (df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(60).mean()).values
    s3c = (df['price_to_SMA_60'] > 1).values
    v3ok = (df['intraday_return'].rolling(90).mean().notna()
            & df['overnight_return'].rolling(60).mean().notna()
            & df['price_to_SMA_60'].notna()).values

    k1 = s1a.astype(int) * 4 + s1b.astype(int) * 2 + s1c.astype(int)
    k2 = s2a.astype(int) * 4 + s2b.astype(int) * 2 + s2c.astype(int)
    k3 = s3a.astype(int) * 4 + s3b.astype(int) * 2 + s3c.astype(int)

    # ---- PIT expanding 查表（標籤滯後 2 天：label[j] 需 open[j+1], open[j+2]）----
    def pit_votes(keys, valid):
        sums = np.zeros(8)
        cnts = np.zeros(8)
        votes = np.zeros(n, dtype=np.int8)
        for i in range(n):
            j = i - 2
            if j >= 0 and valid[j] and not np.isnan(label[j]):
                sums[keys[j]] += label[j]
                cnts[keys[j]] += 1
            if valid[i]:
                er = sums[keys[i]] / cnts[keys[i]] if cnts[keys[i]] > 0 else 0.0
                votes[i] = 1 if er > 0 else -1
        return votes

    vote1 = pit_votes(k1, v1ok)
    vote2 = pit_votes(k2, v2ok)
    vote3 = pit_votes(k3, v3ok)

    # ---- 票4（與 v1 相同規則）----
    us = (h - np.maximum(o, c)) / o
    ir = (h - l) / o
    us_s = pd.Series(us).rolling(2).mean().values
    us_l = pd.Series(us).rolling(10).mean().values
    ir_s = pd.Series(ir).rolling(5).mean().values
    ir_l = pd.Series(ir).rolling(10).mean().values
    v4ok = ~(np.isnan(us_l) | np.isnan(ir_l))
    ts = ((us_s > us_l) & v4ok).astype(np.int8) + ((ir_s > ir_l) & v4ok).astype(np.int8)
    vote4 = np.where(v4ok, np.where(ts == 2, 1, np.where(ts == 0, -1, 0)), 0).astype(np.int8)

    total = (vote1 + vote2 + vote3 + vote4).astype(np.int8)
    target_votes = np.where(total >= 2, 1, np.where(total <= -2, -1, 0)).astype(np.int8)

    # ---- 趨勢訊號：t-1 收盤狀態 → t 日部位（多/空手）----
    above = _shift1((adjC > sma_trend).astype(float))
    sma_ok = ~np.isnan(_shift1(sma_trend))
    target_trend = np.where(sma_ok & (above == 1), 1, 0).astype(np.int8)

    # ---- LT 紙上追蹤艙：大額交易人特定法人淨部位 vs 20日均（多/空，跟隨）----
    # 研究紀錄: 與趨勢/四票相關性 ~0、dev Sharpe 0.54；組合因 MDD 條款未過 dev，
    # 故僅紙上追蹤累積實盤外樣本，非交易建議（見 factor_research6.py）
    net5 = (df['t5_spec_long'] - df['t5_spec_short']).values.astype(float)
    lt_dev = net5 - pd.Series(net5).rolling(LT_N).mean().values
    lt_next = np.where(np.isnan(lt_dev), 0, np.where(lt_dev > 0, 1, -1)).astype(np.int8)  # 明日應持
    target_lt = np.zeros(n, dtype=np.int8)
    target_lt[1:] = lt_next[:-1]                                                          # 當日應持（執行用）

    # ---- 結算日 ----
    is_settle = np.zeros(n, dtype=bool)
    is_settle[:-1] = dm[:-1] != dm[1:]

    # ---- 回測引擎（與 v1 相同會計；target 由呼叫端對齊好「當日應持部位」）----
    def backtest(daily_target):
        positions = np.zeros(n, dtype=np.int8)
        pnls = np.zeros(n)
        pos = 0
        ep = np.nan
        for i in range(1, n):
            pnl = 0.0
            if is_settle[i]:
                if pos == 1:
                    pnl = o[i] - ep
                elif pos == -1:
                    pnl = ep - o[i]
                pos = 0
                ep = np.nan
            else:
                tp = daily_target[i]
                if tp == 1:
                    if pos == -1:
                        pnl += ep - o[i]; ep = o[i]
                    elif pos == 0:
                        ep = o[i]
                    pos = 1; pnl += c[i] - ep; ep = c[i]
                elif tp == -1:
                    if pos == 1:
                        pnl += o[i] - ep; ep = o[i]
                    elif pos == 0:
                        ep = o[i]
                    pos = -1; pnl += ep - c[i]; ep = c[i]
                else:
                    if pos == 1:
                        pnl += o[i] - ep
                    elif pos == -1:
                        pnl += ep - o[i]
                    pos = 0; ep = np.nan
            positions[i] = pos
            pnls[i] = pnl
        return positions, pnls

    # 四票制：T-1 盤後訊號 → T 日執行（與 v1 相同的 shift）
    exec_votes = np.zeros(n, dtype=np.int8)
    exec_votes[1:] = target_votes[:-1]
    pos_v, pnl_v = backtest(exec_votes)
    # 趨勢：target_trend 已是「當日應持部位」（用 t-1 收盤判斷）
    pos_t, pnl_t = backtest(target_trend)
    # LT 紙上追蹤
    pos_l, pnl_l = backtest(target_lt)

    # ---- 輸出（2020 起，與 v1 視窗對齊）----
    mask = (df['trading_date'] >= OUTPUT_START).values
    result = pd.DataFrame({
        'trading_date': df.loc[mask, 'trading_date'].dt.strftime('%Y-%m-%d'),
        'close_price': c[mask],
        'vote1': vote1[mask], 'vote2': vote2[mask],
        'vote3': vote3[mask], 'vote4': vote4[mask],
        'total_votes': total[mask],
        'votes_target': target_votes[mask],      # 四票制：明日應持部位
        'votes_position': pos_v[mask],
        'votes_pnl': pnl_v[mask],
        'votes_cum_pnl': pnl_v[mask].cumsum(),
        'trend_target': np.where(sma_ok, (adjC > sma_trend), False).astype(np.int8)[mask],  # 趨勢：明日應持部位
        'trend_position': pos_t[mask],
        'trend_pnl': pnl_t[mask],
        'trend_cum_pnl': pnl_t[mask].cumsum(),
        'lt_target': lt_next[mask],   # LT 紙上追蹤：明日應持部位
        'lt_position': pos_l[mask],
        'lt_pnl': pnl_l[mask],
        'lt_cum_pnl': pnl_l[mask].cumsum(),
        'updated_at': datetime.now().isoformat(),
    })
    return result


def save_to_db(result_df):
    conn = sqlite3.connect(SIGNAL_DB)
    result_df.to_sql('daily_signals_v2', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    print("[signal-v2] Computing PIT-honest signals...")
    result = compute_signals_v2()
    save_to_db(result)

    last = result.iloc[-1]
    amap = {1: 'LONG', 0: 'FLAT', -1: 'SHORT'}
    print(f"[signal-v2] {len(result)} rows saved to daily_signals_v2 ({result['trading_date'].iloc[0]} ~ {result['trading_date'].iloc[-1]})")
    print(f"[signal-v2] cum PnL (gross, 2020+): votes={last['votes_cum_pnl']:,.0f}  trend={last['trend_cum_pnl']:,.0f}  lt(paper)={last['lt_cum_pnl']:,.0f}")
    print(
        f"[signal-v2] Latest {last['trading_date']} | "
        f"4votes: {int(last['vote1']):+d}/{int(last['vote2']):+d}/{int(last['vote3']):+d}/{int(last['vote4']):+d} "
        f"total={int(last['total_votes']):+d} -> tomorrow {amap[int(last['votes_target'])]} | "
        f"SMA{SMA_N} trend -> tomorrow {amap[int(last['trend_target'])]} | "
        f"LT paper -> tomorrow {amap[int(last['lt_target'])]}"
    )
