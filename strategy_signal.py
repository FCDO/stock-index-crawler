"""
台指期四票制策略 - 每日訊號計算
從本地 SQLite 資料庫讀取，計算四票投票訊號，結果存入 strategy_signal.db
"""
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TX_DB = os.path.join(BASE_DIR, 'tx_futures.db')
INDEX_DB = os.path.join(BASE_DIR, 'stock_index.db')
SIGNAL_DB = os.path.join(BASE_DIR, 'strategy_signal.db')


def compute_signals():
    """計算四票制策略訊號，回傳 DataFrame"""

    # ---- 載入資料 ----
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

    df['trading_date'] = pd.to_datetime(df['trading_date'])
    tpex['date'] = pd.to_datetime(tpex['date'])
    tpex['tpex_daily_return'] = tpex['tpex_close'].pct_change()

    df = df.merge(
        tpex[['date', 'tpex_daily_return']],
        left_on='trading_date', right_on='date', how='inner'
    ).drop(columns='date')

    # ---- 基本報酬（全資料計算）----
    df['daily_return_open'] = df['open_price'].pct_change()
    df['daily_return_open_next_two_day'] = df['daily_return_open'].shift(-2)
    df['overnight_return'] = (
        (df['open_price'] - df['close_price'].shift(1))
        / df['close_price'].shift(1) * 100
    )
    df['intraday_return'] = (
        (df['close_price'] - df['open_price']) / df['open_price'] * 100
    )

    # ---- Warm-up 指標（全資料計算後再篩選）----
    df['SMA_60'] = df['close_price'].rolling(60).mean()
    df['price_to_SMA_60'] = df['close_price'] / df['SMA_60']
    df['tpex_std_20'] = df['tpex_daily_return'].rolling(20).std()

    df = df[df['trading_date'] >= '2020-01-01'].reset_index(drop=True)
    df = df.dropna(subset=['tpex_daily_return']).reset_index(drop=True)

    # ---- 策略1 因子 ----
    irs = df['intraday_return'].rolling(20).std()
    df['s1_a'] = np.where(
        irs.rolling(30).mean() > irs.rolling(50).mean(),
        '短期>長期', '短期<=長期'
    )
    df['s1_b'] = np.where(
        df['tpex_std_20'].rolling(20).mean() > df['tpex_std_20'].rolling(80).mean(),
        '櫃買短期>長期', '櫃買短期<=長期'
    )
    df['s1_c'] = np.where(
        df['overnight_return'].rolling(40).mean() > 0,
        '隔夜報酬>0', '隔夜報酬<=0'
    )

    # ---- 策略2 因子 ----
    df['s2_a'] = np.where(
        df['intraday_return'].rolling(40).mean() > df['intraday_return'].rolling(90).mean(),
        '短日內>長日內', '短日內<=長日內'
    )
    df['s2_b'] = np.where(
        df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(80).mean(),
        '短隔夜>長隔夜', '短隔夜<=長隔夜'
    )
    df['s2_c'] = np.where(
        df['overnight_return'].rolling(40).mean() > df['intraday_return'].rolling(40).mean(),
        '隔夜>日內', '隔夜<=日內'
    )

    # ---- 策略3 因子 ----
    df['s3_a'] = np.where(
        df['intraday_return'].rolling(30).mean() > df['intraday_return'].rolling(90).mean(),
        '短日內>長日內', '短日內<=長日內'
    )
    df['s3_b'] = np.where(
        df['overnight_return'].rolling(40).mean() > df['overnight_return'].rolling(60).mean(),
        '短隔夜>長隔夜', '短隔夜<=長隔夜'
    )
    df['s3_c'] = np.where(df['price_to_SMA_60'] > 1, '價格>季線', '價格<=季線')

    # ---- 策略4 因子 ----
    us = (df['high_price'] - df[['open_price', 'close_price']].max(axis=1)) / df['open_price']
    us_s = us.rolling(2).mean()
    us_l = us.rolling(10).mean()
    ir = (df['high_price'] - df['low_price']) / df['open_price']
    ir_s = ir.rolling(5).mean()
    ir_l = ir.rolling(10).mean()

    # ---- 投票計算 ----
    n = len(df)

    gr1 = df.groupby(['s1_a', 's1_b', 's1_c'])[
        'daily_return_open_next_two_day'
    ].mean().to_dict()
    gr2 = df.groupby(['s2_a', 's2_b', 's2_c'])[
        'daily_return_open_next_two_day'
    ].mean().to_dict()

    # Vote 1
    v1v = (df['s1_a'].notna() & df['s1_b'].notna() & df['s1_c'].notna()).values
    er1 = np.array([gr1.get(k, 0) for k in zip(df['s1_a'], df['s1_b'], df['s1_c'])])
    vote1 = np.where(v1v, np.where(er1 > 0, 1, -1), 0).astype(np.int8)

    # Vote 2
    v2v = (df['s2_a'].notna() & df['s2_b'].notna() & df['s2_c'].notna()).values
    er2 = np.array([gr2.get(k, 0) for k in zip(df['s2_a'], df['s2_b'], df['s2_c'])])
    vote2 = np.where(v2v, np.where(er2 > 0, 1, -1), 0).astype(np.int8)

    # Vote 3 (np.select)
    v3v = (df['s3_a'].notna() & df['s3_b'].notna() & df['s3_c'].notna()).values
    a3, b3, c3 = df['s3_a'].values, df['s3_b'].values, df['s3_c'].values
    conds = [
        (a3 == '短日內>長日內') & (b3 == '短隔夜>長隔夜') & (c3 == '價格>季線'),
        (a3 == '短日內>長日內') & (b3 == '短隔夜>長隔夜') & (c3 != '價格>季線'),
        (a3 == '短日內>長日內') & (b3 != '短隔夜>長隔夜') & (c3 == '價格>季線'),
        (a3 == '短日內>長日內') & (b3 != '短隔夜>長隔夜') & (c3 != '價格>季線'),
        (a3 != '短日內>長日內') & (b3 == '短隔夜>長隔夜') & (c3 == '價格>季線'),
        (a3 != '短日內>長日內') & (b3 == '短隔夜>長隔夜') & (c3 != '價格>季線'),
        (a3 != '短日內>長日內') & (b3 != '短隔夜>長隔夜') & (c3 == '價格>季線'),
        (a3 != '短日內>長日內') & (b3 != '短隔夜>長隔夜') & (c3 != '價格>季線'),
    ]
    vals = [0.001157, -0.001072, 0.000314, -0.004241,
            0.001899, 0.001945, -0.001060, 0.002373]
    er3 = np.select(conds, vals, default=0.0)
    vote3 = np.where(v3v, np.where(er3 > 0, 1, -1), 0).astype(np.int8)

    # Vote 4
    us_up = (us_s > us_l).values & us_s.notna().values & us_l.notna().values
    ir_up = (ir_s > ir_l).values & ir_s.notna().values & ir_l.notna().values
    ts = us_up.astype(np.int8) + ir_up.astype(np.int8)
    vote4 = np.where(ts == 2, 1, np.where(ts == 0, -1, 0)).astype(np.int8)

    total = (vote1 + vote2 + vote3 + vote4).astype(np.int8)
    target = np.where(total >= 2, 1, np.where(total <= -2, -1, 0)).astype(np.int8)

    # ---- 結算日判定（近月合約換月 = 結算日）----
    dm = df['delivery_month'].values
    is_settlement = np.zeros(n, dtype=bool)
    is_settlement[:-1] = dm[:-1] != dm[1:]

    # ---- 回測 ----
    open_arr = df['open_price'].values.astype(np.float64)
    close_arr = df['close_price'].values.astype(np.float64)
    positions = np.zeros(n, dtype=np.int8)
    daily_pnls = np.zeros(n, dtype=np.float64)

    pos = 0
    ep = np.nan
    for i in range(1, n):
        o, c = open_arr[i], close_arr[i]
        pnl = 0.0

        if is_settlement[i]:
            # 結算日：開盤結清現有部位，不建新倉
            if pos == 1:
                pnl = o - ep
            elif pos == -1:
                pnl = ep - o
            pos = 0
            ep = np.nan
        else:
            tp = target[i - 1]
            if tp == 1:
                if pos == -1:
                    pnl += ep - o; ep = o
                elif pos == 0:
                    ep = o
                pos = 1; pnl += c - ep; ep = c
            elif tp == -1:
                if pos == 1:
                    pnl += o - ep; ep = o
                elif pos == 0:
                    ep = o
                pos = -1; pnl += ep - c; ep = c
            else:
                if pos == 1:
                    pnl += o - ep
                elif pos == -1:
                    pnl += ep - o
                pos = 0; ep = np.nan

        positions[i] = pos
        daily_pnls[i] = pnl

    result = pd.DataFrame({
        'trading_date': df['trading_date'].dt.strftime('%Y-%m-%d'),
        'close_price': df['close_price'].values,
        'vote1': vote1, 'vote2': vote2, 'vote3': vote3, 'vote4': vote4,
        'total_votes': total, 'tech_score': ts,
        'target_position': target,
        'position': positions,
        'daily_pnl': daily_pnls,
        'cumulative_pnl': daily_pnls.cumsum(),
        'updated_at': datetime.now().isoformat(),
    })
    return result


def save_to_db(result_df):
    """存入 strategy_signal.db"""
    conn = sqlite3.connect(SIGNAL_DB)
    result_df.to_sql('daily_signals', conn, if_exists='replace', index=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT,
            rows INTEGER
        )
    """)
    conn.execute(
        "INSERT INTO update_log (updated_at, rows) VALUES (?, ?)",
        (datetime.now().isoformat(), len(result_df))
    )
    conn.commit()
    conn.close()


if __name__ == '__main__':
    print("[signal] Computing strategy signals...")
    result = compute_signals()
    save_to_db(result)

    last = result.iloc[-1]
    action_map = {1: '做多', 0: '空手', -1: '做空'}
    print(f"[signal] {len(result)} rows saved to {SIGNAL_DB}")
    print(
        f"[signal] Latest: {last['trading_date']} | "
        f"votes={int(last['total_votes']):+d} | "
        f"tomorrow={action_map[last['target_position']]}"
    )
