# -*- coding: utf-8 -*-
"""
signal_ledger.py - append-only 訊號帳本（前瞻紙上實盤的證據層）

為什麼需要：
  daily_signals_v2 每天整表重算重寫（if_exists='replace'）——未來任何程式修改或
  上游資料修訂都會無聲改寫「歷史訊號」，無法作為事前承諾的 LT 12 個月紙上實盤證據。
  本帳本對 signal_ledger.db 只做 INSERT OR IGNORE：同一 (signal_date, sleeve)
  第一次寫入後永不更動。signal_ledger.db 獨立成檔（雲端 workflow 不寫它）、
  由 daily_update.bat 每日 git commit，形成可稽核的時間戳鏈。

事前承諾的評估法（--report 寫死，不留調整空間）：
  - 部位：signal_date T 盤後記錄的 target（明日應持），自下一交易日開盤生效；
    若某日漏記（停機），沿用最後已知 target（與實盤行為一致），缺口列入報告
  - 結算日開盤強制平倉、次日依最新 target 重新進場（與 factor_research* 會計相同）
  - 成本：每邊 1.5 點
  - 乾淨窗起點：2026-06-12（CLAUDE.md 既定承諾——LT 測試窗於 2026-06-11 意外
    揭露後，採納決策只能依據其後的前瞻記錄）。此前寫入的列僅作上下文
  - LT 採納門檻（2027-06-12 滿 12 個月後方可判定）：
    實盤 LT Sharpe >= 0.3 且 與趨勢艙日損益相關性 <= 0.2 → 方可考慮半口納入
"""
import sqlite3
import os
import sys
from datetime import datetime, date

BASE = os.path.dirname(os.path.abspath(__file__))
SIGNAL_DB = os.path.join(BASE, 'strategy_signal.db')
LEDGER_DB = os.path.join(BASE, 'signal_ledger.db')
TX_DB = os.path.join(BASE, 'tx_futures.db')

CLEAN_START = '2026-06-12'   # 乾淨窗起點（事前承諾，不可改）
GATE_DATE = '2027-06-12'     # LT 採納資格判定日（滿 12 個月）
COST = 1.5                   # 每邊成本（點），與研究回測一致
SLEEVES = ('votes_v1', 'votes_v2', 'trend', 'lt')


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_ledger (
            signal_date TEXT NOT NULL,   -- 訊號計算基準日（資料最新交易日）
            sleeve      TEXT NOT NULL,   -- votes_v1 / votes_v2 / trend / lt
            target      INTEGER NOT NULL,-- 次一交易日應持部位 (1/0/-1)
            close_price REAL,            -- 基準日近月收盤（上下文）
            captured_at TEXT NOT NULL,   -- 寫入時間（ISO）
            is_backfill INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (signal_date, sleeve)
        )
    """)


def capture():
    """讀取 v1/v2 最新訊號，僅新增、永不改寫。"""
    src = sqlite3.connect(SIGNAL_DB)
    v2 = src.execute("""SELECT trading_date, votes_target, trend_target, lt_target, close_price
                        FROM daily_signals_v2 ORDER BY trading_date DESC LIMIT 1""").fetchone()
    v1 = src.execute("""SELECT trading_date, target_position, close_price
                        FROM daily_signals ORDER BY trading_date DESC LIMIT 1""").fetchone()
    src.close()

    now = datetime.now().isoformat(timespec='seconds')
    rows = []
    if v2:
        d, votes_t, trend_t, lt_t, cp = v2
        rows += [(d, 'votes_v2', int(votes_t), cp, now),
                 (d, 'trend', int(trend_t), cp, now),
                 (d, 'lt', int(lt_t), cp, now)]
    if v1:
        d1, tgt1, cp1 = v1
        rows.append((d1, 'votes_v1', int(tgt1), cp1, now))

    led = sqlite3.connect(LEDGER_DB)
    ensure_table(led)
    inserted = 0
    for r in rows:
        cur = led.execute("""INSERT OR IGNORE INTO signal_ledger
            (signal_date, sleeve, target, close_price, captured_at, is_backfill)
            VALUES (?,?,?,?,?,0)""", r)
        inserted += cur.rowcount
    led.commit()
    total = led.execute("SELECT COUNT(*) FROM signal_ledger").fetchone()[0]
    led.close()
    print(f"[ledger] {inserted} new rows inserted (skipped {len(rows)-inserted} existing), total {total}")
    if rows:
        print(f"[ledger] latest signal_date={rows[0][0]} targets: " +
              " ".join(f"{r[1]}={r[2]:+d}" for r in rows))


def report():
    """前瞻記錄評估（乾淨窗 2026-06-12 起）。會計規則見檔頭，寫死。"""
    import pandas as pd
    import numpy as np

    led = sqlite3.connect(LEDGER_DB)
    ensure_table(led)
    lg = pd.read_sql_query(
        "SELECT signal_date, sleeve, target FROM signal_ledger WHERE signal_date >= ? ORDER BY signal_date",
        led, params=(CLEAN_START,))
    led.close()

    conn = sqlite3.connect(TX_DB)
    px = pd.read_sql_query("""
        WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
                   FROM tx_futures WHERE session='一般')
        SELECT trading_date, open_price, close_price, delivery_month FROM R WHERE rn=1
        ORDER BY trading_date""", conn)
    conn.close()
    px['d'] = pd.to_datetime(px['trading_date']).dt.strftime('%Y-%m-%d')
    px = px[px['d'] >= CLEAN_START].reset_index(drop=True)

    n = len(px)
    today = date.today().isoformat()
    months = max(0.0, (pd.Timestamp(today) - pd.Timestamp(CLEAN_START)).days / 30.44)
    print(f"[ledger-report] clean window {CLEAN_START} ~ {today} ({months:.1f} months, gate at {GATE_DATE})")
    if n < 2 or lg.empty:
        print("[ledger-report] not enough data yet - keep accumulating.")
        return

    o = px['open_price'].values.astype(float)
    c = px['close_price'].values.astype(float)
    dm = px['delivery_month'].values
    settle = np.zeros(n, dtype=bool)
    settle[:-1] = dm[:-1] != dm[1:]

    piv = lg.pivot(index='signal_date', columns='sleeve', values='target')
    pnl_streams = {}
    for sleeve in SLEEVES:
        if sleeve not in piv.columns:
            continue
        tmap = piv[sleeve].dropna()
        # sig[t] = 部位於交易日 t：取「< 當日」最後一筆已知 target（漏記則沿用）
        sig = np.zeros(n)
        last = 0.0
        gaps = 0
        dates = px['d'].values
        ti = 0
        tdates = tmap.index.to_list()
        tvals = tmap.values
        for t in range(n):
            while ti < len(tdates) and tdates[ti] < dates[t]:
                last = float(tvals[ti])
                ti += 1
            if t > 0 and (ti == 0 or tdates[ti - 1] != dates[t - 1]):
                gaps += 1
            sig[t] = last
        # 研究版會計
        pnl = np.zeros(n)
        pos = 0
        ep = np.nan
        for t in range(1, n):
            p, tc = 0.0, 0.0
            if settle[t]:
                if pos == 1:
                    p = o[t] - ep
                elif pos == -1:
                    p = ep - o[t]
                if pos != 0:
                    tc = COST
                pos, ep = 0, np.nan
            else:
                tgt = int(sig[t])
                if tgt != pos:
                    if pos == 1:
                        p += o[t] - ep
                    elif pos == -1:
                        p += ep - o[t]
                    tc += COST * abs(tgt - pos)
                    pos = tgt
                    ep = o[t] if pos != 0 else np.nan
                if pos == 1:
                    p += c[t] - ep
                    ep = c[t]
                elif pos == -1:
                    p += ep - c[t]
                    ep = c[t]
            pnl[t] = p - tc
        pnl_streams[sleeve] = pnl
        sd = pnl.std()
        sh = pnl.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
        cum = pnl.cumsum()
        mdd = (cum - np.maximum.accumulate(cum)).min()
        print(f"  {sleeve:<9} days={n-1}  net={pnl.sum():>8,.0f}  Sharpe={sh:>5.2f}  MDD={mdd:>8,.0f}  missed_capture_days={gaps}")

    if 'lt' in pnl_streams and 'trend' in pnl_streams:
        a, b = pnl_streams['lt'], pnl_streams['trend']
        corr = np.corrcoef(a, b)[0, 1] if a.std() > 0 and b.std() > 0 else float('nan')
        sd = a.std()
        lt_sh = a.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
        ready = today >= GATE_DATE
        print(f"[gate] LT adoption check: months={months:.1f}/12  liveSharpe={lt_sh:.2f} (need >=0.3)  "
              f"corr(lt,trend)={corr:.2f} (need <=0.2)  -> {'ELIGIBLE TO JUDGE' if ready else 'NOT YET (wait until ' + GATE_DATE + ')'}")


if __name__ == '__main__':
    if '--report' in sys.argv:
        report()
    else:
        capture()
