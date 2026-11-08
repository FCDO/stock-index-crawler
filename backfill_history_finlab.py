# -*- coding: utf-8 -*-
"""
institutional.db 歷史回補（一次性；不在每日排程內；可重跑、冪等）— 2026-11-08

期交所三大法人頁面／下載現在只開放近 3 年（2026-11 實測 futContractsDate 回「無法查詢2023/11/08日以前資料」、
futContractsDateDown 回「日期時間錯誤」），FinMind TaiwanFuturesInstitutionalInvestors 最早 2018-06-05。
更早的歷史改由下列來源補：

  inst   FinLab futures_institutional_investors_trading_summary（2007-07-02 起，期交所三大法人統計的轉載）
         臺股期貨 → fut_institutional、小型臺指期貨 → fut_institutional_mtx。
         只寫 INST_CUTOFF（2018-06-05）之前的日期，INSERT OR IGNORE，既有 FinMind／期交所列一列都不碰。
         寫入前閘門（任一不過就中止、不寫）：
           1. 與既有列重疊期 long_oi／short_oi／net_oi 逐列 0 差異（成交量差異只列出、不擋）
           2. 回補區間每個 TX 交易日（tx_futures.db 一般盤）都在、每日 3 種身分、沒有多餘日期
           3. long−short＝net（未平倉與成交量）、無 NaN、皆為整數、多空口數不為負
  mtxoi  期交所 futDataDown（commodity_id=MTX；此端點仍提供完整歷史）→ mtx_oi，
         只寫 MTXOI_CUTOFF（2018-01-02，FinMind 起點）之前的日期；彙總邏輯同 institutional_crawler.save_mtx_oi。
         閘門：重抓 2018-01～03 與既有列 mkt_oi／mkt_oi_monthly 逐日相同才開始寫。
         已有完整資料的月份自動跳過（中斷可續跑）。
  clean  刪除法人表中「非 TX 交易日、與前一個交易日 3 身分 6 欄完全相同、且在已審名單 DUP_DATES_REVIEWED 內」的複本列
         （2026-11-08 已刪：fut_institutional 2026-04-11～06-07 的 18 個週六日＋2026-05-01 勞動節休市，共 19 日 57 列，
         都是 2026-06-11 FinMind 重補產生；週六的 2007-09-29～2018-12-22 是補班交易日、為真資料）。
         不在名單內的候選只列出、不刪（審過再加進名單）。
  check  三張表的涵蓋／完整性報告（交易日缺漏、身分數、long−short＝net、法人合計 ≤ 全市場未平倉、新舊銜接處）。

用法：python backfill_history_finlab.py [inst|mtxoi|clean|check|all] [--dry-run]
FinLab token：環境變數 FINLAB_API_TOKEN，否則讀 ..\\LTR PROJECT\\.env（同 台指期_電金比/base_data.read_token）。
FinLab 用 CacheStorage（只在記憶體），不會在 repo 內產生 finlab_db 資料夾。
輸出只用 ASCII（cp950 終端相容）。
"""
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import institutional_crawler as IC  # noqa: E402  (fetch_csv / _num / FUT_DATA_URL; import has no side effects)

INST_DB = HERE / "institutional.db"
TX_DB = HERE / "tx_futures.db"

INST_CUTOFF = pd.Timestamp("2018-06-05")    # FinMind TX/MTX 3-investor data starts here
MTXOI_CUTOFF = pd.Timestamp("2018-01-02")   # FinMind TaiwanFuturesDaily MTX (mtx_oi) starts here
FL_START = pd.Timestamp("2007-07-02")       # FinLab / TAIFEX 3-investor history starts here
MTXOI_OVERLAP = ((2018, 1), (2018, 2), (2018, 3))
SLEEP = 7
# 已審過、確認是 FinMind 2026-06-11 重補產生的非交易日複本（與前一個週五／4-30 逐欄相同）
DUP_DATES_REVIEWED = frozenset({
    "2026-04-11", "2026-04-12", "2026-04-18", "2026-04-19", "2026-04-25", "2026-04-26",
    "2026-05-02", "2026-05-03", "2026-05-09", "2026-05-10", "2026-05-16", "2026-05-17",
    "2026-05-23", "2026-05-24", "2026-05-30", "2026-05-31", "2026-06-06", "2026-06-07",
    "2026-05-01",                                   # 勞動節（期交所休市）
})

FL_TABLE = "futures_institutional_investors_trading_summary"
FL_FIELDS = {"long_vol": "多方交易口數", "short_vol": "空方交易口數", "net_vol": "多空交易口數淨額",
             "long_oi": "多方未平倉口數", "short_oi": "空方未平倉口數", "net_oi": "多空未平倉口數淨額"}
FL_COMM = {"fut_institutional": "臺股期貨", "fut_institutional_mtx": "小型臺指期貨"}
FL_IDENT = {"外資及陸資": "foreign", "外資": "foreign", "投信": "trust", "自營商": "dealer"}
COLS = ["long_vol", "short_vol", "net_vol", "long_oi", "short_oi", "net_oi"]


# ───────────────────────── helpers ─────────────────────────
def ro(path):
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)


def tx_calendar():
    """TX regular-session trading days (tx_futures.db)."""
    con = ro(TX_DB)
    d = pd.read_sql("SELECT DISTINCT trading_date FROM tx_futures WHERE session='一般'", con)["trading_date"]
    con.close()
    return pd.DatetimeIndex(pd.to_datetime(d.str.strip(), format="%Y/%m/%d")).sort_values()


def tx_market_oi():
    """TX whole-market OI per day = sum over all contract months (regular session, spreads excluded)."""
    con = ro(TX_DB)
    q = ("SELECT trading_date d, SUM(CAST(open_interest AS INTEGER)) oi FROM tx_futures "
         "WHERE session='一般' AND TRIM(contract)='TX' AND delivery_month NOT LIKE '%/%' GROUP BY trading_date")
    s = pd.read_sql(q, con)
    con.close()
    return pd.Series(s["oi"].values, index=pd.to_datetime(s["d"].str.strip(), format="%Y/%m/%d")).sort_index()


def read_finlab_token():
    tok = os.environ.get("FINLAB_API_TOKEN")
    if tok:
        return tok
    env = HERE.parent / "LTR PROJECT" / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("FINLAB_API_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("[ERR] FinLab token not found (env FINLAB_API_TOKEN or ..\\LTR PROJECT\\.env)")


def load_inst_table(con, table):
    df = pd.read_sql(f"SELECT date, identity, {', '.join(COLS)} FROM {table}", con)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ───────────────────────── inst（FinLab）─────────────────────────
def load_finlab_long():
    """FinLab wide tables (date x '<commodity>_<identity>') -> long frames per target table."""
    import finlab
    from finlab import data
    data.set_storage(data.CacheStorage())        # memory only
    finlab.login(read_finlab_token())
    parts = {t: [] for t in FL_COMM}
    for k, name in FL_FIELDS.items():
        wide = pd.DataFrame(data.get(f"{FL_TABLE}:{name}"))
        wide.index = pd.DatetimeIndex(wide.index)
        for table, comm in FL_COMM.items():
            cols = [c for c in wide.columns if str(c).startswith(comm + "_")]
            sub = wide[cols].copy()
            sub.columns = [FL_IDENT[str(c).split("_", 1)[1]] for c in cols]
            if sorted(sub.columns) != ["dealer", "foreign", "trust"]:
                raise SystemExit(f"[ERR] {table} {k}: unexpected identity columns {list(sub.columns)}")
            lng = sub.rename_axis("date").reset_index().melt(id_vars="date", var_name="identity", value_name=k)
            parts[table].append(lng.set_index(["date", "identity"])[k])
    out = {}
    for table, ser in parts.items():
        df = pd.concat(ser, axis=1).reset_index().sort_values(["date", "identity"]).reset_index(drop=True)
        out[table] = df
    return out


def gate_inst(con, table, fl, cal):
    """Return (rows_to_insert, ok)."""
    ok = True
    ex = load_inst_table(con, table)
    print(f"\n[{table}] existing {len(ex)} rows {ex['date'].min().date()}..{ex['date'].max().date()} | "
          f"FinLab {len(fl)} rows {fl['date'].min().date()}..{fl['date'].max().date()}")
    if ex["date"].min() < INST_CUTOFF:
        print(f"  [INFO] table already has rows before {INST_CUTOFF.date()} (re-run): INSERT OR IGNORE keeps them")
    # 1. overlap with existing FinMind/TAIFEX rows
    mg = ex[ex["date"] >= INST_CUTOFF].merge(fl, on=["date", "identity"], suffixes=("_db", "_fl"))
    print(f"  gate1 overlap rows compared: {len(mg)} ({mg['date'].min().date()}..{mg['date'].max().date()})")
    if len(mg) < 5000:
        print("  [FAIL] overlap too small")
        ok = False
    for f in COLS:
        bad = mg[mg[f + "_db"] != mg[f + "_fl"]]
        tag = "FAIL" if (f.endswith("_oi") and len(bad)) else ("note" if len(bad) else "ok")
        print(f"    {f:9s} mismatches {len(bad):4d}  [{tag}]")
        if len(bad):
            for r in bad.head(10).itertuples(index=False):
                print(f"      {r.date.date()} {r.identity:7s} db={getattr(r, f + '_db')} finlab={getattr(r, f + '_fl')}")
        if f.endswith("_oi") and len(bad):
            ok = False
    # backfill slice
    new = fl[fl["date"] < INST_CUTOFF].copy()
    if new["date"].min() != FL_START:
        print(f"  [FAIL] FinLab first date {new['date'].min().date()} != {FL_START.date()}")
        ok = False
    # 3. internal identities
    n_nan = int(new[COLS].isna().sum().sum())
    n_frac = int((new[COLS].fillna(0) % 1 != 0).sum().sum())
    n_neg = int((new[["long_vol", "short_vol", "long_oi", "short_oi"]] < 0).sum().sum())
    n_oi = int((new["long_oi"] - new["short_oi"] != new["net_oi"]).sum())
    n_vol = int((new["long_vol"] - new["short_vol"] != new["net_vol"]).sum())
    print(f"  gate3 NaN {n_nan} | non-integer {n_frac} | negative {n_neg} | long-short!=net oi {n_oi} vol {n_vol}")
    if n_nan or n_frac or n_neg or n_oi or n_vol:
        ok = False
    # 2. calendar & identities
    per_day = new.groupby("date")["identity"].nunique()
    win = cal[(cal >= FL_START) & (cal < INST_CUTOFF)]
    nd = pd.DatetimeIndex(per_day.index)
    miss, extra = win.difference(nd), nd.difference(win)
    not3 = int((per_day != 3).sum())
    print(f"  gate2 TX trading days {FL_START.date()}..{(INST_CUTOFF - timedelta(days=1)).date()}: {len(win)} | "
          f"FinLab days {len(nd)} | missing {len(miss)} {[str(d.date()) for d in miss[:10]]} | "
          f"extra {len(extra)} {[str(d.date()) for d in extra[:10]]} | days with !=3 identities {not3}")
    if len(miss) or len(extra) or not3:
        ok = False
    print(f"  => {'PASS' if ok else 'FAIL'}: {len(new)} rows to insert ({new['date'].min().date()}.."
          f"{new['date'].max().date()})")
    return new, ok


def run_inst(dry):
    cal = tx_calendar()
    fl = load_finlab_long()
    con = sqlite3.connect(INST_DB)
    plans, all_ok = {}, True
    for table in FL_COMM:
        new, ok = gate_inst(con, table, fl[table], cal)
        plans[table] = new
        all_ok &= ok
    if not all_ok:
        con.close()
        raise SystemExit("[ABORT] inst gates failed -> nothing written")
    if dry:
        print("\n[dry-run] inst: gates passed, nothing written")
        con.close()
        return
    ts = datetime.now().isoformat()
    for table, new in plans.items():
        rows = [(r.date.strftime("%Y-%m-%d"), r.identity, *(int(getattr(r, c)) for c in COLS), ts)
                for r in new.itertuples(index=False)]
        before = con.total_changes
        con.executemany(f"INSERT OR IGNORE INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.commit()
        print(f"[{table}] inserted {con.total_changes - before} of {len(rows)} rows (rest already present)")
    con.close()


# ───────────────────────── mtxoi（期交所 futDataDown）─────────────────────────
def agg_mtx_rows(rows):
    """Same aggregation as institutional_crawler.save_mtx_oi + save_mtx_oi_records (without writing)."""
    days = {}
    for row in rows[1:]:
        if len(row) < 18 or "/" not in row[0] or row[1].strip() != "MTX":
            continue
        if row[17].strip() != "一般":
            continue
        code, oi = str(row[2]).strip(), IC._num(row[11])
        if not code or "/" in code or oi is None:
            continue
        rec = days.setdefault(row[0].strip().replace("/", "-"), [0, 0, 0])
        rec[0] += oi
        if "W" not in code:
            rec[1] += oi
        rec[2] += 1
    return days


def fetch_mtx_month(y, m):
    s = datetime(y, m, 1)
    e = (datetime(y + 1, 1, 1) if m == 12 else datetime(y, m + 1, 1)) - timedelta(days=1)
    print(f"  {s:%Y/%m}", end="", flush=True)
    rows = IC.fetch_csv(IC.FUT_DATA_URL, {"down_type": "1", "commodity_id": "MTX",
                                          "queryStartDate": s.strftime("%Y/%m/%d"),
                                          "queryEndDate": e.strftime("%Y/%m/%d")}, "mtx_oi")
    time.sleep(SLEEP)
    return agg_mtx_rows(rows) if rows else None


def run_mtxoi(dry):
    cal = tx_calendar()
    con = sqlite3.connect(INST_DB)
    ex = pd.read_sql("SELECT date, mkt_oi, mkt_oi_monthly, n_contracts FROM mtx_oi", con).set_index("date")
    print(f"[mtx_oi] existing {len(ex)} rows {ex.index.min()}..{ex.index.max()}")
    # gate: re-fetched overlap months must equal stored rows
    print("[mtx_oi] gate: overlap months vs stored rows")
    ok = True
    for y, m in MTXOI_OVERLAP:
        days = fetch_mtx_month(y, m)
        if not days:
            print(" -> no data [FAIL]")
            ok = False
            continue
        got = pd.DataFrame(days, index=["mkt_oi", "mkt_oi_monthly", "n_contracts"]).T.sort_index()
        mg = got.join(ex, how="outer", lsuffix="_tf", rsuffix="_db")
        mg = mg[mg.index.str.startswith(f"{y}-{m:02d}")]
        bad_oi = int(((mg["mkt_oi_tf"] != mg["mkt_oi_db"]) | (mg["mkt_oi_monthly_tf"] != mg["mkt_oi_monthly_db"])).sum())
        bad_n = int((mg["n_contracts_tf"] != mg["n_contracts_db"]).sum())
        print(f" -> {len(got)} days | oi mismatches {bad_oi} | n_contracts mismatches {bad_n} (note only)")
        if bad_oi:
            print(mg[(mg["mkt_oi_tf"] != mg["mkt_oi_db"])].head(10).to_string())
            ok = False
    if not ok:
        con.close()
        raise SystemExit("[ABORT] mtx_oi overlap gate failed -> nothing written")
    # backfill months FL_START..(MTXOI_CUTOFF - 1 day)
    have = set(pd.to_datetime(ex.index))
    y, m = FL_START.year, FL_START.month
    last = cal[cal < MTXOI_CUTOFF].max()                # last trading day before the FinMind start (2017-12-29)
    total_ins = 0
    print(f"[mtx_oi] backfill {FL_START:%Y-%m}..{last:%Y-%m}" + (" (dry-run: first month only)" if dry else ""))
    while (y, m) <= (last.year, last.month):
        mdays = cal[(cal.year == y) & (cal.month == m) & (cal >= FL_START) & (cal < MTXOI_CUTOFF)]
        if len(mdays) and all(d in have for d in mdays):
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
            continue                                    # month already complete (resume)
        days = fetch_mtx_month(y, m)
        if not days:
            print(" -> 0 [WARN] no data")
        else:
            got = {pd.Timestamp(d): v for d, v in days.items() if FL_START <= pd.Timestamp(d) < MTXOI_CUTOFF}
            gd = pd.DatetimeIndex(sorted(got))
            miss, extra = mdays.difference(gd), gd.difference(mdays)
            flag = "" if not len(miss) and not len(extra) else \
                f" [WARN] missing {[str(d.date()) for d in miss]} extra {[str(d.date()) for d in extra]}"
            ncs = sorted({v[2] for v in got.values()})
            if dry:
                print(f" -> {len(got)} days, n_contracts {ncs}{flag} (dry-run, not written)")
                break
            ts = datetime.now().isoformat()
            before = con.total_changes
            con.executemany("INSERT OR IGNORE INTO mtx_oi VALUES (?, ?, ?, ?, ?)",
                            [(d.strftime("%Y-%m-%d"), v[0], v[1], v[2], ts) for d, v in sorted(got.items())])
            con.commit()
            ins = con.total_changes - before
            total_ins += ins
            print(f" -> {len(got)} days, inserted {ins}, n_contracts {ncs}{flag}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    con.close()
    print(f"[mtx_oi] inserted total {total_ins}")


# ───────────────────────── clean（非交易日複本）─────────────────────────
def run_clean(dry):
    cal = tx_calendar()
    calset = set(cal)
    con = sqlite3.connect(INST_DB)
    for table in FL_COMM:
        df = load_inst_table(con, table)
        sat = sorted(d for d in df["date"].unique() if pd.Timestamp(d).weekday() >= 5 and pd.Timestamp(d) in calset)
        cand = sorted(d for d in df["date"].unique() if pd.Timestamp(d) not in calset)
        dup, review = [], []
        for d in cand:
            d = pd.Timestamp(d)
            prev = cal[cal < d].max()
            a = df[df["date"] == d].set_index("identity")[COLS].sort_index()
            b = df[df["date"] == prev].set_index("identity")[COLS].sort_index()
            same = a.shape == (3, 6) and b.shape == (3, 6) and a.equals(b)
            if same and d.strftime("%Y-%m-%d") in DUP_DATES_REVIEWED:
                dup.append(d)
            else:
                review.append((d, f"copy of {prev.date()}" if same else f"differs from {prev.date()}"))
        print(f"[clean] {table}: non-TX-trading dates {len(cand)} | reviewed duplicates to delete {len(dup)} | "
              f"Saturday make-up trading days kept {len(sat)}")
        if dup:
            print("  delete:", [str(d.date()) for d in dup])
        for d, why in review:
            print(f"  [REVIEW] {d.date()} not deleted ({why}; not in DUP_DATES_REVIEWED)")
        if dup and not dry:
            before = con.total_changes
            con.executemany(f"DELETE FROM {table} WHERE date = ?", [(d.strftime("%Y-%m-%d"),) for d in dup])
            con.commit()
            print(f"  deleted {con.total_changes - before} rows")
    con.close()


# ───────────────────────── check ─────────────────────────
def run_check():
    cal = tx_calendar()
    con = ro(INST_DB)
    txoi = tx_market_oi()
    mo = pd.read_sql("SELECT date, mkt_oi, mkt_oi_monthly, n_contracts FROM mtx_oi", con)
    mo["date"] = pd.to_datetime(mo["date"])
    mo = mo.set_index("date").sort_index()
    print("=" * 78)
    for table, mkt in (("fut_institutional", txoi), ("fut_institutional_mtx", mo["mkt_oi"])):
        df = load_inst_table(con, table)
        g = df.groupby("date")
        d = pd.DatetimeIndex(g.size().index)
        win = cal[(cal >= d.min()) & (cal <= d.max())]
        miss, extra = win.difference(d), d.difference(cal)
        s_long, s_short = g["long_oi"].sum(), g["short_oi"].sum()
        mk = mkt.reindex(d)
        print(f"[{table}] {len(df)} rows | {d.min().date()}..{d.max().date()} | {len(d)} days "
              f"(TX trading days in range {len(win)})")
        print(f"  TX trading days missing {len(miss)} {[str(x.date()) for x in miss[:12]]}")
        print(f"  dates not TX trading days {len(extra)} {[str(x.date()) for x in extra[:12]]}")
        print(f"  days with !=3 identities {int((g.size() != 3).sum())} | long-short!=net oi "
              f"{int((df['long_oi'] - df['short_oi'] != df['net_oi']).sum())} vol "
              f"{int((df['long_vol'] - df['short_vol'] != df['net_vol']).sum())} | NaN {int(df[COLS].isna().sum().sum())}")
        print(f"  market OI missing for {int(mk.isna().sum())} days | 3-investor long sum > market OI "
              f"{int((s_long > mk).sum())} | short sum > market OI {int((s_short > mk).sum())}")
        share = pd.DataFrame({"long": s_long / mk, "short": s_short / mk})
        ys = share.groupby(share.index.year).median()
        print("  median 3-investor share of market OI by year (long/short): " +
              " ".join(f"{y}:{r.long:.2f}/{r.short:.2f}" for y, r in ys.iterrows()))
        seam = df[(df["date"] >= INST_CUTOFF - timedelta(days=7)) & (df["date"] <= INST_CUTOFF + timedelta(days=6))]
        piv = seam.pivot(index="date", columns="identity", values="net_oi")
        print("  seam net_oi (FinLab <2018-06-05 | FinMind >=):")
        for dt, r in piv.iterrows():
            print(f"    {dt.date()} {'<' if dt < INST_CUTOFF else '>='} foreign {r['foreign']:>8.0f} "
                  f"trust {r['trust']:>8.0f} dealer {r['dealer']:>8.0f}")
        yr = pd.DataFrame({"days": pd.Series(1, index=d).groupby(d.year).sum(),
                           "tx_days": pd.Series(1, index=win).groupby(win.year).sum()}).fillna(0).astype(int)
        bad = yr[yr["days"] != yr["tx_days"]]
        print(f"  years where days != TX trading days: {bad.to_dict('index') if len(bad) else 'none'}")
        print("-" * 78)
    win = cal[(cal >= mo.index.min()) & (cal <= mo.index.max())]
    miss, extra = win.difference(mo.index), mo.index.difference(cal)
    print(f"[mtx_oi] {len(mo)} rows | {mo.index.min().date()}..{mo.index.max().date()} | TX trading days in range {len(win)}")
    print(f"  TX trading days missing {len(miss)} {[str(x.date()) for x in miss[:12]]}")
    print(f"  dates not TX trading days {len(extra)} {[str(x.date()) for x in extra[:12]]}")
    print(f"  mkt_oi<=0 {int((mo['mkt_oi'] <= 0).sum())} | monthly>all {int((mo['mkt_oi_monthly'] > mo['mkt_oi']).sum())} | "
          f"weekly share>0 before 2018: {int(((mo['mkt_oi'] != mo['mkt_oi_monthly']) & (mo.index < MTXOI_CUTOFF)).sum())}")
    seam = mo[(mo.index >= MTXOI_CUTOFF - timedelta(days=8)) & (mo.index <= MTXOI_CUTOFF + timedelta(days=6))]
    print("  seam (TAIFEX <2018-01-02 | FinMind >=): " +
          " ".join(f"{d.date()}:{int(v)}" for d, v in seam["mkt_oi"].items()))
    jumps = mo["mkt_oi"].pct_change().abs()
    print("  largest day-over-day |%chg| in mkt_oi: " +
          " ".join(f"{d.date()}:{v:.0%}" for d, v in jumps.nlargest(5).items()))
    ym = mo.groupby(mo.index.year)["mkt_oi"].median()
    print("  median mkt_oi by year: " + " ".join(f"{y}:{int(v)}" for y, v in ym.items()))
    con.close()
    print("=" * 78)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    step = args[0] if args else "all"
    if step not in ("inst", "mtxoi", "clean", "check", "all"):
        raise SystemExit(__doc__)
    if step in ("inst", "all"):
        run_inst(dry)
    if step in ("clean", "all"):
        run_clean(dry)
    if step in ("mtxoi", "all"):
        run_mtxoi(dry)
    if step in ("check", "all"):
        run_check()


if __name__ == "__main__":
    main()
