# -*- coding: utf-8 -*-
"""
TAIFEX / TWSE institutional & flows data crawler -> institutional.db (dates YYYY-MM-DD)

Tables:
- fut_institutional : TX futures 3-investor positions  (FinMind backfill 2018-06+, TAIFEX daily; TAIFEX serves last 3y only)
- pc_ratio          : TXO put/call ratio               (TAIFEX, full history 2007/07+, 1-month query cap)
- spot_flows        : TWSE market-wide investor buy/sell, NT$ (FinMind backfill 2007+, TWSE BFI82U daily)
- margin_totals     : TWSE market margin balances       (FinMind backfill 2001+, TWSE MI_MARGN daily)
- large_trader_tx   : TAIFEX TX large trader top5/10 OI (TAIFEX largeTraderFutDown, full history 2004+; month=999999 rows)

TAIFEX throttles: 6s spacing, HTML response = throttled -> long backoff (except pre-data months).
"""
import sqlite3
import time
import csv
import io
import requests
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings()

DB_PATH = "institutional.db"
FUT_URL = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
PC_URL = "https://www.taifex.com.tw/cht/3/pcRatioDown"
LT_URL = "https://www.taifex.com.tw/cht/3/largeTraderFutDown"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
PC_DATA_START = datetime(2007, 7, 1)
LT_DATA_START = datetime(2004, 1, 1)
FINMIND_FUT_START = 2018
FINMIND_SPOT_START = 2007
FINMIND_MARGIN_START = 2001
SLEEP = 6

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9",
})
TWSE = requests.Session()
TWSE.verify = False
TWSE.headers.update({"User-Agent": "Mozilla/5.0"})


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fut_institutional (
            date TEXT, identity TEXT,
            long_vol INTEGER, short_vol INTEGER, net_vol INTEGER,
            long_oi INTEGER, short_oi INTEGER, net_oi INTEGER,
            updated_at TEXT, PRIMARY KEY (date, identity)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pc_ratio (
            date TEXT PRIMARY KEY,
            put_vol INTEGER, call_vol INTEGER, pc_vol_ratio REAL,
            put_oi INTEGER, call_oi INTEGER, pc_oi_ratio REAL,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spot_flows (
            date TEXT PRIMARY KEY,
            foreign_net REAL, trust_net REAL, dealer_net REAL, total_net REAL,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS margin_totals (
            date TEXT PRIMARY KEY,
            margin_lots REAL, short_lots REAL, margin_money REAL,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS large_trader_tx (
            date TEXT PRIMARY KEY,
            t5_all_long INTEGER, t5_all_short INTEGER,
            t5_spec_long INTEGER, t5_spec_short INTEGER,
            t10_all_long INTEGER, t10_all_short INTEGER,
            t10_spec_long INTEGER, t10_spec_short INTEGER,
            mkt_oi INTEGER, updated_at TEXT
        )
    """)
    conn.commit()
    return conn


def norm_identity(raw):
    if "外資" in raw:
        return "foreign"
    if "自營" in raw:
        return "dealer"
    if "投信" in raw:
        return "trust"
    return raw


def _num(s):
    s = str(s).strip().replace(",", "")
    if s in ("", "-", "None"):
        return None
    return int(float(s))


def _fnum(s):
    s = str(s).strip().replace(",", "")
    return float(s) if s not in ("", "-", "None") else None


# ================= FinMind backfills =================
def _finmind(dataset, y1, y2, extra=None):
    p = {"dataset": dataset, "start_date": f"{y1}-01-01", "end_date": f"{y2}-12-31"}
    if extra:
        p.update(extra)
    r = requests.get(FINMIND_URL, params=p, timeout=120)
    return r.json().get("data", [])


def backfill_finmind_fut(conn):
    if conn.execute("SELECT COUNT(*) FROM fut_institutional WHERE date < '2023-01-01'").fetchone()[0] > 1000:
        print("[finmind fut] already backfilled, skip")
        return
    now = datetime.now()
    total = 0
    for y in range(FINMIND_FUT_START, now.year + 1):
        try:
            data = _finmind("TaiwanFuturesInstitutionalInvestors", y, y, {"data_id": "TX"})
        except Exception as e:
            print(f"[finmind fut] {y} [WARN] {e}")
            continue
        ts = datetime.now().isoformat()
        for d in data:
            lv, sv = _num(d["long_deal_volume"]), _num(d["short_deal_volume"])
            lo, so = _num(d["long_open_interest_balance_volume"]), _num(d["short_open_interest_balance_volume"])
            conn.execute("""
                INSERT OR REPLACE INTO fut_institutional
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (d["date"], norm_identity(d["institutional_investors"]), lv, sv, lv - sv, lo, so, lo - so, ts))
        conn.commit()
        total += len(data)
        time.sleep(2)
    print(f"[finmind fut] {total} rows")


def backfill_finmind_spot(conn):
    if conn.execute("SELECT COUNT(*) FROM spot_flows WHERE date < '2023-01-01'").fetchone()[0] > 1000:
        print("[finmind spot] already backfilled, skip")
        return
    now = datetime.now()
    total = 0
    for y in range(FINMIND_SPOT_START, now.year + 1):
        try:
            data = _finmind("TaiwanStockTotalInstitutionalInvestors", y, y)
        except Exception as e:
            print(f"[finmind spot] {y} [WARN] {e}")
            continue
        ts = datetime.now().isoformat()
        days = {}
        for d in data:
            rec = days.setdefault(d["date"], {"foreign": 0.0, "trust": 0.0, "dealer": 0.0, "total": 0.0})
            net = (d["buy"] or 0) - (d["sell"] or 0)
            nm = d["name"]
            if "Foreign" in nm:
                rec["foreign"] += net
            elif "Investment_Trust" in nm:
                rec["trust"] += net
            elif "Dealer" in nm:
                rec["dealer"] += net
            rec["total"] += net
        for dte, rec in days.items():
            conn.execute("INSERT OR REPLACE INTO spot_flows VALUES (?, ?, ?, ?, ?, ?)",
                         (dte, rec["foreign"], rec["trust"], rec["dealer"], rec["total"], ts))
        conn.commit()
        total += len(days)
        time.sleep(2)
    print(f"[finmind spot] {total} days")


def backfill_finmind_margin(conn):
    if conn.execute("SELECT COUNT(*) FROM margin_totals WHERE date < '2020-01-01'").fetchone()[0] > 1000:
        print("[finmind margin] already backfilled, skip")
        return
    now = datetime.now()
    total = 0
    for y in range(FINMIND_MARGIN_START, now.year + 1):
        try:
            data = _finmind("TaiwanStockTotalMarginPurchaseShortSale", y, y)
        except Exception as e:
            print(f"[finmind margin] {y} [WARN] {e}")
            continue
        ts = datetime.now().isoformat()
        days = {}
        for d in data:
            rec = days.setdefault(d["date"], {})
            rec[d["name"]] = d.get("TodayBalance")
        for dte, rec in days.items():
            conn.execute("INSERT OR REPLACE INTO margin_totals VALUES (?, ?, ?, ?, ?)",
                         (dte, rec.get("MarginPurchase"), rec.get("ShortSale"),
                          rec.get("MarginPurchaseMoney"), ts))
        conn.commit()
        total += len(days)
        time.sleep(2)
    print(f"[finmind margin] {total} days")


# ================= TWSE daily increments =================
def twse_daily(conn):
    """Fill spot_flows / margin_totals for days after the latest stored date (TWSE direct)."""
    now = datetime.now()
    for table, fetch in (("spot_flows", _twse_bfi82u), ("margin_totals", _twse_margn)):
        row = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
        start = datetime.strptime(row[0], "%Y-%m-%d") + timedelta(days=1) if row[0] else None
        if start is None:
            continue
        d = start
        cnt = 0
        while d <= now:
            if d.weekday() < 5:
                try:
                    if fetch(conn, d):
                        cnt += 1
                except Exception as e:
                    print(f"[twse {table}] {d.date()} [WARN] {e}")
                time.sleep(3)
            d += timedelta(days=1)
        print(f"[twse {table}] +{cnt} days")


def _twse_bfi82u(conn, d):
    r = TWSE.get("https://www.twse.com.tw/rwd/zh/fund/BFI82U",
                 params={"type": "day", "dayDate": d.strftime("%Y%m%d"), "response": "json"}, timeout=60)
    j = r.json()
    if j.get("stat") != "OK" or not j.get("data"):
        return False
    rec = {"foreign": 0.0, "trust": 0.0, "dealer": 0.0, "total": 0.0}
    for nm, _b, _s, diff in j["data"]:
        v = _fnum(diff) or 0.0
        if "合計" in nm:
            rec["total"] = v
        elif "外資" in nm:
            rec["foreign"] += v
        elif "投信" in nm:
            rec["trust"] += v
        elif "自營" in nm:
            rec["dealer"] += v
    conn.execute("INSERT OR REPLACE INTO spot_flows VALUES (?, ?, ?, ?, ?, ?)",
                 (d.strftime("%Y-%m-%d"), rec["foreign"], rec["trust"], rec["dealer"],
                  rec["total"], datetime.now().isoformat()))
    conn.commit()
    return True


def _twse_margn(conn, d):
    r = TWSE.get("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",
                 params={"date": d.strftime("%Y%m%d"), "selectType": "MS", "response": "json"}, timeout=60)
    j = r.json()
    tables = j.get("tables") or []
    if j.get("stat") != "OK" or not tables or not tables[0].get("data"):
        return False
    margin_lots = short_lots = margin_money = None
    for row in tables[0]["data"]:
        nm = row[0]
        bal = _fnum(row[5])
        if nm.startswith("融資(交易單位)"):
            margin_lots = bal
        elif nm.startswith("融券(交易單位)"):
            short_lots = bal
        elif nm.startswith("融資金額"):
            margin_money = bal
    conn.execute("INSERT OR REPLACE INTO margin_totals VALUES (?, ?, ?, ?, ?)",
                 (d.strftime("%Y-%m-%d"), margin_lots, short_lots, margin_money,
                  datetime.now().isoformat()))
    conn.commit()
    return True


# ================= TAIFEX fetch =================
def fetch_csv(url, payload, tag, short_retry=False):
    waits = (20,) if short_retry else (60, 120, 300)
    for attempt in range(len(waits) + 1):
        try:
            r = SESSION.post(url, data=payload, timeout=90)
            if r.status_code != 200 or not r.content.strip():
                raise Exception(f"HTTP {r.status_code} / empty")
            text = r.content.decode("big5", errors="replace")
            if text.lstrip().startswith("<"):
                raise Exception("throttled/no-data (HTML)")
            return list(csv.reader(io.StringIO(text)))
        except Exception as e:
            if attempt < len(waits):
                print(f" (retry {attempt+1}: wait {waits[attempt]}s)", end="", flush=True)
                time.sleep(waits[attempt])
            else:
                print(f" [WARN] {tag}: {e}", end="")
                return None


def save_fut(conn, rows):
    ts = datetime.now().isoformat()
    count = 0
    for row in rows[1:]:
        if len(row) < 15 or "/" not in row[0]:
            continue
        conn.execute("INSERT OR REPLACE INTO fut_institutional VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (row[0].strip().replace("/", "-"), norm_identity(row[2].strip()),
                      _num(row[3]), _num(row[5]), _num(row[7]),
                      _num(row[9]), _num(row[11]), _num(row[13]), ts))
        count += 1
    conn.commit()
    return count


def save_pc(conn, rows):
    ts = datetime.now().isoformat()
    count = 0
    for row in rows[1:]:
        if len(row) < 7 or "/" not in row[0]:
            continue
        conn.execute("INSERT OR REPLACE INTO pc_ratio VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (row[0].strip().replace("/", "-"),
                      _num(row[1]), _num(row[2]), _fnum(row[3]),
                      _num(row[4]), _num(row[5]), _fnum(row[6]), ts))
        count += 1
    conn.commit()
    return count


def save_large_trader(conn, rows):
    """Keep TX rows with month==999999 (all contracts); cat 0=all large traders, 1=specific institutions."""
    ts = datetime.now().isoformat()
    days = {}
    for row in rows[1:]:
        if len(row) < 10 or "/" not in row[0]:
            continue
        if row[1].strip() != "TX" or row[3].strip() != "999999":
            continue
        d = row[0].strip().replace("/", "-")
        cat = row[4].strip()
        rec = days.setdefault(d, {})
        rec[cat] = (_num(row[5]), _num(row[6]), _num(row[7]), _num(row[8]), _num(row[9]))
    for d, rec in days.items():
        a = rec.get("0", (None,) * 5)
        s = rec.get("1", (None,) * 5)
        conn.execute("INSERT OR REPLACE INTO large_trader_tx VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (d, a[0], a[1], s[0], s[1], a[2], a[3], s[2], s[3], a[4] or s[4], ts))
    conn.commit()
    return len(days)


def month_starts(start, end):
    cur = datetime(start.year, start.month, 1)
    while cur <= end:
        nxt = datetime(cur.year + 1, 1, 1) if cur.month == 12 else datetime(cur.year, cur.month + 1, 1)
        yield max(cur, start), min(nxt - timedelta(days=1), end)
        cur = nxt


def crawl_taifex(conn, table, url, payload_fn, save_fn, default_start):
    row = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
    now = datetime.now()
    has_data = bool(row[0])
    if has_data:
        latest = datetime.strptime(row[0], "%Y-%m-%d")
        start = datetime(latest.year, latest.month, 1)
    else:
        start = default_start
    print(f"[{table}] TAIFEX from {start.strftime('%Y/%m')} to {now.strftime('%Y/%m')}")
    total = 0
    for s, e in month_starts(start, now):
        s_str, e_str = s.strftime("%Y/%m/%d"), e.strftime("%Y/%m/%d")
        print(f"  {s_str}~{e_str}", end="", flush=True)
        rows = fetch_csv(url, payload_fn(s_str, e_str), table, short_retry=not has_data)
        if rows:
            cnt = save_fn(conn, rows)
            total += cnt
            if cnt:
                has_data = True
            print(f" -> {cnt}")
        else:
            print(" -> 0")
        time.sleep(SLEEP)
    print(f"[{table}] inserted/updated: {total}")


def main():
    print("=" * 50)
    print("TAIFEX/TWSE Institutional & Flows Crawler")
    print("=" * 50)
    conn = init_db()

    # 1) FinMind history backfills (run once each)
    backfill_finmind_fut(conn)
    backfill_finmind_spot(conn)
    backfill_finmind_margin(conn)

    # 2) TWSE daily increments (spot flows + margin)
    twse_daily(conn)

    # 3) TAIFEX: fut institutional incremental (3y window), pc_ratio + large trader (full history)
    row = conn.execute("SELECT MAX(date) FROM fut_institutional").fetchone()
    if row[0]:
        latest = datetime.strptime(row[0], "%Y-%m-%d")
        crawl_taifex(conn, "fut_institutional", FUT_URL,
                     lambda s, e: {"queryStartDate": s, "queryEndDate": e, "commodityId": "TXF"},
                     save_fut, datetime(latest.year, latest.month, 1))
    crawl_taifex(conn, "pc_ratio", PC_URL,
                 lambda s, e: {"queryStartDate": s, "queryEndDate": e},
                 save_pc, PC_DATA_START)
    crawl_taifex(conn, "large_trader_tx", LT_URL,
                 lambda s, e: {"queryStartDate": s, "queryEndDate": e},
                 save_large_trader, LT_DATA_START)

    for t in ("fut_institutional", "pc_ratio", "spot_flows", "margin_totals", "large_trader_tx"):
        cnt, lo, hi = conn.execute(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {t}").fetchone()
        print(f"  {t}: {cnt} rows ({lo} ~ {hi})")
    conn.close()
    print("=" * 50)


if __name__ == "__main__":
    main()
