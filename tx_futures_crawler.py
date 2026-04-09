"""
台指期貨歷史資料爬蟲
- 資料來源: 台灣期貨交易所 (TAIFEX) futDataDown
- 商品: TX (臺股期貨)
- 資料範圍: DB 最新日期 ~ 至今 (空 DB 則從 2000/01 開始)
- 儲存格式: SQLite (tx_futures.db)
"""

import sqlite3
import time
import csv
import io
import requests
from datetime import datetime, timedelta

DB_PATH = "tx_futures.db"
TAIFEX_URL = "https://www.taifex.com.tw/cht/3/futDataDown"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9",
})

# TAIFEX CSV column indices (big5 encoded, 19 columns)
COL_TRADING_DATE = 0
COL_CONTRACT = 1
COL_DELIVERY_MONTH = 2
COL_OPEN = 3
COL_HIGH = 4
COL_LOW = 5
COL_CLOSE = 6
COL_CHANGE = 7
COL_CHANGE_PCT = 8
COL_VOLUME = 9
COL_SETTLEMENT = 10
COL_OI = 11
COL_SESSION = 17

FIELD_INDICES = [
    COL_TRADING_DATE, COL_CONTRACT, COL_DELIVERY_MONTH,
    COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE,
    COL_CHANGE, COL_CHANGE_PCT, COL_VOLUME,
    COL_SETTLEMENT, COL_OI, COL_SESSION,
]


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tx_futures (
            trading_date TEXT,
            contract TEXT,
            delivery_month TEXT,
            open_price INTEGER,
            high_price INTEGER,
            low_price INTEGER,
            close_price INTEGER,
            price_change INTEGER,
            change_percent REAL,
            volume INTEGER,
            settlement_price INTEGER,
            open_interest INTEGER,
            session TEXT,
            PRIMARY KEY (trading_date, contract, delivery_month, session)
        )
    """)
    conn.commit()
    return conn


def get_latest_date(conn):
    row = conn.execute("SELECT MAX(trading_date) FROM tx_futures").fetchone()
    if row[0]:
        return datetime.strptime(row[0], "%Y/%m/%d")
    return None


def download_and_save(conn, start_str, end_str):
    """Download TX futures CSV for date range and insert into DB.
    Returns number of rows inserted.
    """
    print(f"  {start_str} ~ {end_str}", end="", flush=True)

    for attempt in range(3):
        try:
            r = SESSION.post(TAIFEX_URL, data={
                "down_type": "1",
                "commodity_id": "TX",
                "queryStartDate": start_str,
                "queryEndDate": end_str,
            }, timeout=60)

            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")

            if not r.content.strip():
                raise Exception("empty response")

            break
        except Exception as e:
            if attempt < 2:
                wait = 10 * (attempt + 1)
                print(f" (retry {attempt+1}, wait {wait}s)", end="", flush=True)
                time.sleep(wait)
            else:
                print(f" [WARN] {e}")
                return 0

    text = r.content.decode("big5", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    if len(rows) < 2:
        print(" (no data)")
        return 0

    count = 0
    for row in rows[1:]:
        if len(row) < 18:
            continue

        delivery_month = row[COL_DELIVERY_MONTH].strip()

        # Skip spread contracts (contain '/')
        if "/" in delivery_month:
            continue

        # Extract the 13 fields, store as-is (raw CSV values) for compatibility
        values = tuple(row[i].strip() for i in FIELD_INDICES)

        conn.execute("""
            INSERT OR REPLACE INTO tx_futures
            (trading_date, contract, delivery_month, open_price, high_price,
             low_price, close_price, price_change, change_percent, volume,
             settlement_price, open_interest, session)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
        count += 1

    conn.commit()
    print(f" -> {count} rows")
    return count


def main():
    print("=" * 50)
    print("TX Futures Crawler (TAIFEX)")
    print("=" * 50)
    print()

    conn = init_db()
    latest = get_latest_date(conn)
    now = datetime.now()

    if latest is None:
        print("DB is empty, starting from 2000/01")
        start = datetime(2000, 1, 1)
    else:
        start = latest + timedelta(days=1)
        if start > now:
            print("DB is up to date")
            conn.close()
            return
        print(f"Updating from {start.strftime('%Y/%m/%d')} (latest: {latest.strftime('%Y/%m/%d')})")

    print()
    total = 0
    current = start
    while current <= now:
        if current.month == 12:
            month_end = datetime(current.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(current.year, current.month + 1, 1) - timedelta(days=1)

        end = min(month_end, now)

        total += download_and_save(
            conn,
            current.strftime("%Y/%m/%d"),
            end.strftime("%Y/%m/%d"),
        )
        time.sleep(3)

        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    # Stats
    row_count = conn.execute("SELECT COUNT(*) FROM tx_futures").fetchone()[0]
    rng = conn.execute(
        "SELECT MIN(trading_date), MAX(trading_date) FROM tx_futures"
    ).fetchone()
    print()
    print(f"  Total: {row_count} rows ({rng[0]} ~ {rng[1]})")
    print(f"  DB: {DB_PATH}")
    print("=" * 50)

    conn.close()


if __name__ == "__main__":
    main()
