"""
上市櫃指數歷史資料爬蟲
- 加權指數 (TWSE): OHLC + 成交金額(股票合計, 不含 ETF/權證)
- 櫃買指數 (TPEx): OHLC + 成交金額(含全部上櫃證券)
- 資料範圍: 2000/01 ~ 至今
- 儲存格式: SQLite

成交金額來源:
  TWSE: MI_INDEX 大盤統計資訊「股票合計」(2004/02/11 起)
        FMTQIK 全市場(2004 以前, ETF/權證成交量可忽略)
  TPEx: tradingIndexRpk (含全部上櫃證券, 含盤後/零股/鉅額)
"""

import sqlite3
import time
import requests
import urllib3
from datetime import datetime, date

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_PATH = "stock_index.db"
SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

# MI_INDEX 可用起始日
MI_INDEX_START = "2004-02-11"


# ---------- 資料庫 ----------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS twse_index (
            date       TEXT PRIMARY KEY,
            open       REAL,
            high       REAL,
            low        REAL,
            close      REAL,
            trading_volume   REAL,
            trading_value    REAL,
            transactions     INTEGER,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tpex_index (
            date       TEXT PRIMARY KEY,
            open       REAL,
            high       REAL,
            low        REAL,
            close      REAL,
            trading_volume   REAL,
            trading_value    REAL,
            transactions     INTEGER,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawl_progress (
            task TEXT,
            date TEXT,
            PRIMARY KEY (task, date)
        )
    """)
    conn.commit()
    return conn


def get_latest_date(conn, table):
    row = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
    return row[0] if row[0] else None


# ---------- 解析工具 ----------

def parse_number(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip().replace(",", "")
    if not s or s == "--":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def roc_to_iso(roc_date_str):
    roc_date_str = roc_date_str.strip()
    parts = roc_date_str.split("/")
    if len(parts) != 3:
        return None
    year = int(parts[0]) + 1911
    return f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"


def western_to_iso(date_str):
    return date_str.strip().replace("/", "-")


def api_get_json(url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 200 and r.text.strip():
                return r.json()
        except Exception:
            pass
        if attempt < max_retries - 1:
            wait = 10 * (attempt + 1)
            print(f" (retry {attempt+1}, wait {wait}s)", end="", flush=True)
            time.sleep(wait)
    return None


# ---------- TWSE 加權指數 ----------

def fetch_twse_ohlc(year, month):
    """MI_5MINS_HIST: 每月加權指數 OHLC"""
    url = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
    params = {"date": f"{year}{month:02d}01", "response": "json"}
    d = api_get_json(url, params)
    if not d or d.get("stat") != "OK" or not d.get("data"):
        return []
    rows = []
    for row in d["data"]:
        iso_date = roc_to_iso(row[0])
        if iso_date:
            rows.append({
                "date": iso_date,
                "open": parse_number(row[1]),
                "high": parse_number(row[2]),
                "low": parse_number(row[3]),
                "close": parse_number(row[4]),
            })
    return rows


def fetch_twse_trading_fmtqik(year, month):
    """FMTQIK: 全市場成交金額 (2004 以前使用)"""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
    params = {"date": f"{year}{month:02d}01", "response": "json"}
    d = api_get_json(url, params)
    if not d or d.get("stat") != "OK" or not d.get("data"):
        return []
    rows = []
    for row in d["data"]:
        iso_date = roc_to_iso(row[0])
        if iso_date:
            rows.append({
                "date": iso_date,
                "trading_volume": parse_number(row[1]),
                "trading_value": parse_number(row[2]),
                "transactions": int(parse_number(row[3]) or 0),
            })
    return rows


def fetch_twse_trading_mi_index(date_str):
    """MI_INDEX: 單日大盤統計資訊, 取「股票合計」列 (2004/02/11 起)
    date_str: YYYY-MM-DD
    """
    ymd = date_str.replace("-", "")
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
    params = {"date": ymd, "response": "json", "type": "ALL"}
    d = api_get_json(url, params)
    if not d or d.get("stat") != "OK":
        return None
    tables = d.get("tables", [])
    if len(tables) < 7:
        return None
    tb = tables[6]
    data = tb.get("data", [])
    if not data:
        return None

    # 找「股票合計」列; 若找不到就用最後一列（通常是總計前的合計列）
    stock_row = None
    for row in data:
        label = row[0]
        # 股票合計(1+6+14+15) 或類似格式
        if "+" in label and "1" in label:
            stock_row = row
            break

    if not stock_row:
        # fallback: 用第一列「一般股票」
        stock_row = data[0]

    return {
        "trading_value": parse_number(stock_row[1]),
        "trading_volume": parse_number(stock_row[2]),
        "transactions": int(parse_number(stock_row[3]) or 0),
    }


def crawl_twse(conn, start_year=2000):
    """爬取 TWSE: Phase 1 = OHLC + FMTQIK, Phase 2 = MI_INDEX 修正成交金額"""
    now = date.today()
    total_months = (now.year - start_year) * 12 + now.month

    # --- Phase 1: OHLC + FMTQIK (月批次, 快) ---
    latest = get_latest_date(conn, "twse_index")
    if latest:
        start_y = int(latest[:4])
        start_m = int(latest[5:7])
        print(f"  Phase 1 - OHLC: from {start_y}/{start_m:02d} (latest: {latest})")
    else:
        start_y, start_m = start_year, 1
        print(f"  Phase 1 - OHLC: from {start_y}/{start_m:02d}")

    ohlc_count = 0
    y, m = start_y, start_m
    while y < now.year or (y == now.year and m <= now.month):
        elapsed = (y - start_year) * 12 + m
        print(f"\r  TWSE OHLC {y}/{m:02d} ({elapsed}/{total_months})", end="", flush=True)
        try:
            ohlc_rows = fetch_twse_ohlc(y, m)
            time.sleep(3)
            trading_rows = fetch_twse_trading_fmtqik(y, m)
            time.sleep(3)

            trading_map = {r["date"]: r for r in trading_rows}
            now_str = datetime.now().isoformat()

            for ohlc in ohlc_rows:
                d = ohlc["date"]
                trading = trading_map.get(d, {})
                # 2004 以前: FMTQIK 作為最終成交金額
                # 2004 以後: FMTQIK 暫存, Phase 2 會用 MI_INDEX 覆蓋
                conn.execute("""
                    INSERT OR REPLACE INTO twse_index
                    (date, open, high, low, close, trading_volume, trading_value, transactions, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    d, ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"],
                    trading.get("trading_volume"),
                    trading.get("trading_value"),
                    trading.get("transactions"),
                    now_str,
                ))
                ohlc_count += 1
            conn.commit()
        except Exception as e:
            print(f"\n  [WARN] TWSE {y}/{m:02d} error: {e}")
            time.sleep(5)
        m += 1
        if m > 12:
            m = 1
            y += 1
    print(f"\n  Phase 1 done: {ohlc_count} rows")

    # --- Phase 2: MI_INDEX 修正 2004+ 成交金額 (逐日) ---
    dates = [r[0] for r in conn.execute(
        """SELECT date FROM twse_index
           WHERE date >= ?
             AND date NOT IN (SELECT date FROM crawl_progress WHERE task = 'twse_mi_index')
           ORDER BY date""", (MI_INDEX_START,)
    ).fetchall()]
    print(f"  Phase 2 - MI_INDEX: {len(dates)} days to update")

    updated = 0
    for i, d in enumerate(dates):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"\r  MI_INDEX {d} ({i+1}/{len(dates)})", end="", flush=True)
        try:
            trading = fetch_twse_trading_mi_index(d)
            if trading:
                now_str = datetime.now().isoformat()
                conn.execute("""
                    UPDATE twse_index
                    SET trading_volume=?, trading_value=?, transactions=?, updated_at=?
                    WHERE date=?
                """, (
                    trading["trading_volume"],
                    trading["trading_value"],
                    trading["transactions"],
                    now_str,
                    d,
                ))
                conn.execute(
                    "INSERT OR IGNORE INTO crawl_progress VALUES (?, ?)",
                    ('twse_mi_index', d),
                )
                updated += 1
                if updated % 100 == 0:
                    conn.commit()
        except Exception as e:
            print(f"\n  [WARN] MI_INDEX {d} error: {e}")
        time.sleep(3)

    conn.commit()
    print(f"\n  Phase 2 done: {updated} rows updated")
    return ohlc_count


# ---------- TPEx 櫃買指數 ----------

def fetch_tpex_ohlc(year, month):
    """indexInfo/inx: 每月櫃買指數 OHLC"""
    url = "https://www.tpex.org.tw/www/zh-tw/indexInfo/inx"
    params = {"date": f"{year}/{month:02d}/01", "type": "Monthly"}
    d = api_get_json(url, params)
    if not d or d.get("stat") != "ok" or not d.get("tables"):
        return []
    table = d["tables"][0]
    if not table.get("data"):
        return []
    rows = []
    for row in table["data"]:
        iso_date = western_to_iso(row[0])
        rows.append({
            "date": iso_date,
            "open": parse_number(row[1]),
            "high": parse_number(row[2]),
            "low": parse_number(row[3]),
            "close": parse_number(row[4]),
        })
    return rows


def _parse_tpex_trading_rows(d):
    """Parse tradingIndex / tradingIndexRpk response into rows."""
    if not d or d.get("stat") != "ok" or not d.get("tables"):
        return []
    table = d["tables"][0]
    if not table.get("data"):
        return []
    rows = []
    for row in table["data"]:
        iso_date = roc_to_iso(row[0])
        if iso_date:
            volume_k = parse_number(row[1])
            value_k = parse_number(row[2])
            rows.append({
                "date": iso_date,
                "trading_volume": volume_k * 1000 if volume_k else None,
                "trading_value": value_k * 1000 if value_k else None,
                "transactions": int(parse_number(row[3]) or 0),
            })
    return rows


def fetch_tpex_trading(year, month):
    """tradingIndexRpk (2009+) 含全部上櫃證券; fallback tradingIndex (2000-2008)"""
    params = {"date": f"{year}/{month:02d}/01", "type": "Monthly"}
    # Try tradingIndexRpk first (available from 2009)
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndexRpk"
    rows = _parse_tpex_trading_rows(api_get_json(url, params))
    if rows:
        return rows
    # Fallback to tradingIndex for older dates
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndex"
    return _parse_tpex_trading_rows(api_get_json(url, params))


def crawl_tpex(conn, start_year=2000):
    """爬取 TPEx 櫃買指數 (月批次)"""
    now = date.today()
    total_months = (now.year - start_year) * 12 + now.month
    count = 0

    latest = get_latest_date(conn, "tpex_index")
    if latest:
        start_y = int(latest[:4])
        start_m = int(latest[5:7])
        print(f"  TPEx from {start_y}/{start_m:02d} (latest: {latest})")
    else:
        start_y, start_m = start_year, 1
        print(f"  TPEx from {start_y}/{start_m:02d}")

    y, m = start_y, start_m
    while y < now.year or (y == now.year and m <= now.month):
        elapsed = (y - start_year) * 12 + m
        print(f"\r  TPEx {y}/{m:02d} ({elapsed}/{total_months})", end="", flush=True)
        try:
            ohlc_rows = fetch_tpex_ohlc(y, m)
            time.sleep(3)
            trading_rows = fetch_tpex_trading(y, m)
            time.sleep(3)

            trading_map = {r["date"]: r for r in trading_rows}
            now_str = datetime.now().isoformat()

            for ohlc in ohlc_rows:
                d = ohlc["date"]
                trading = trading_map.get(d, {})
                conn.execute("""
                    INSERT OR REPLACE INTO tpex_index
                    (date, open, high, low, close, trading_volume, trading_value, transactions, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    d, ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"],
                    trading.get("trading_volume"),
                    trading.get("trading_value"),
                    trading.get("transactions"),
                    now_str,
                ))
                count += 1
            conn.commit()
        except Exception as e:
            print(f"\n  [WARN] TPEx {y}/{m:02d} error: {e}")
            time.sleep(5)
        m += 1
        if m > 12:
            m = 1
            y += 1

    print(f"\n  TPEx done: {count} rows")
    return count


# ---------- 主程式 ----------

def main():
    print("=" * 50)
    print("Stock Index Crawler (TWSE + TPEx)")
    print("=" * 50)
    print()

    conn = init_db()

    print("[1/2] TWSE (weighted index)...")
    crawl_twse(conn)
    print()

    print("[2/2] TPEx (OTC index)...")
    crawl_tpex(conn)
    print()

    # stats
    for table, name in [("twse_index", "TWSE"), ("tpex_index", "TPEx")]:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        nulls = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE trading_value IS NULL").fetchone()[0]
        rng = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
        print(f"  {name}: {total} rows ({rng[0]} ~ {rng[1]}), null trading_value: {nulls}")

    print()
    print(f"DB: {DB_PATH}")
    print("=" * 50)
    conn.close()


if __name__ == "__main__":
    main()
