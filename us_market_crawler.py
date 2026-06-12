# -*- coding: utf-8 -*-
"""
us_market_crawler.py - S&P 500 指數日線爬蟲（Yahoo Finance chart API）

- 資料來源：Yahoo Finance 公開 REST endpoint（依 CLAUDE.md 資料取得優先順序第 1 位）
  https://query1.finance.yahoo.com/v8/finance/chart/^GSPC?range=max&interval=1d
- 使用現貨指數 ^GSPC（文獻標準、歷史最長）；不用 ES=F（Yahoo 歷史短、無歷史盤中快照）
- 日期：以 meta.gmtoffset 將 epoch 轉為交易所當地日期（避免 Windows zoneinfo 依賴）
- 儲存：us_market.db / spx_daily（INSERT OR REPLACE，可重跑）
- 時序意義：美股日期 U 的收盤完成於台灣時間 U+1 日清晨 04:00-05:00，
  早於台指期 08:45 開盤 → U 日資料對台股交易日 D 可用之條件為 U <= D-1
"""
import requests
import sqlite3
import os
import urllib3
from datetime import datetime, timedelta, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, 'us_market.db')
URL = 'https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC'


def fetch():
    # range=max 會被 Yahoo 強制降為 3mo 粒度 → 改用 period1/period2 明確指定
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    r = requests.get(
        URL,
        params={'period1': '631152000', 'period2': str(now_epoch), 'interval': '1d'},  # 1990-01-01 起（涵蓋 dev 暖身即可）
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
        timeout=60, verify=False,
    )
    r.raise_for_status()
    j = r.json()
    res = j['chart']['result'][0]
    meta = res['meta']
    gmtoff = int(meta.get('gmtoffset', -4 * 3600))
    ts = res['timestamp']
    q = res['indicators']['quote'][0]
    rows = []
    now = datetime.now().isoformat(timespec='seconds')
    for i, t in enumerate(ts):
        cl = q['close'][i]
        if cl is None:
            continue
        d = datetime.fromtimestamp(t + gmtoff, tz=timezone.utc).date().isoformat()
        rows.append((d, q['open'][i], q['high'][i], q['low'][i], cl, q['volume'][i], now))
    return rows


def save(rows):
    conn = sqlite3.connect(DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spx_daily (
            date TEXT PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            updated_at TEXT
        )""")
    conn.executemany("INSERT OR REPLACE INTO spx_daily VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    lo, hi, cnt = conn.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM spx_daily").fetchone()
    conn.close()
    print(f"[us] spx_daily: {cnt} rows ({lo} ~ {hi}) -> {os.path.basename(DB)}")


if __name__ == '__main__':
    print("[us] Fetching ^GSPC daily history from Yahoo Finance...")
    save(fetch())
