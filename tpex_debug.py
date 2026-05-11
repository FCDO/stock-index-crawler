"""
TPEx connectivity probe for GitHub Actions environment.

Calls the same endpoints that crawler.py uses for TPEx, prints full
diagnostics (status, headers, body preview, JSON parse result) so we can
see what the runner's IP actually gets back. Tries two header variants to
distinguish "crawler-style minimal UA" vs "real browser UA + Referer".

Run via the TPEx Debug workflow (workflow_dispatch).
"""
import sys
import requests

requests.packages.urllib3.disable_warnings()

ENDPOINTS = [
    ("OHLC monthly (indexInfo/inx)",
     "https://www.tpex.org.tw/www/zh-tw/indexInfo/inx",
     {"date": "2026/05/01", "type": "Monthly"}),
    ("Trading monthly Rpk (2009+)",
     "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndexRpk",
     {"date": "2026/05/01", "type": "Monthly"}),
    ("Trading monthly legacy",
     "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndex",
     {"date": "2026/05/01", "type": "Monthly"}),
]

HEADERS_CRAWLER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.tpex.org.tw/",
}


def probe(label, headers):
    print("\n" + "=" * 70)
    print(f"Probe: {label}")
    print(f"UA: {headers.get('User-Agent', '')[:80]}")
    print(f"Referer: {headers.get('Referer', '(none)')}")

    for name, url, params in ENDPOINTS:
        print(f"\n--- {name} ---")
        print(f"URL:    {url}")
        print(f"Params: {params}")
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30, verify=False)
        except Exception as e:
            print(f"REQUEST FAILED: {type(e).__name__}: {e}")
            continue

        print(f"Status:       {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type')}")
        print(f"Server:       {r.headers.get('Server')}")
        print(f"CF-Ray:       {r.headers.get('CF-Ray', '(none)')}")
        print(f"X-Cache:      {r.headers.get('X-Cache', '(none)')}")
        print(f"Body length:  {len(r.content)} bytes")
        preview = r.text[:600].replace("\n", "\\n")
        print(f"Body[:600]:   {preview!r}")

        try:
            d = r.json()
            tables = d.get("tables") or []
            stat = d.get("stat")
            rows = tables[0].get("data") if tables else []
            print(f"JSON OK | stat={stat!r} | tables={len(tables)} | rows={len(rows)}")
            if rows:
                print(f"  First row: {rows[0]}")
                print(f"  Last row:  {rows[-1]}")
        except Exception as e:
            print(f"JSON parse FAILED: {e}")


def main():
    print(f"Python: {sys.version}")
    print("Public IP:", end=" ")
    try:
        ip = requests.get("https://api.ipify.org", timeout=10).text
        print(ip)
    except Exception as e:
        print(f"(lookup failed: {e})")

    probe("CRAWLER UA (matches crawler.py SESSION)", HEADERS_CRAWLER)
    probe("FULL BROWSER UA + Referer", HEADERS_BROWSER)


if __name__ == "__main__":
    main()
