# 上市櫃指數爬蟲 - 開發指南

## 核心原則：第一性原理（First Principles Thinking）

遵循馬斯克第一性原理思維，回歸問題本質，不盲目沿用既有做法。
每個技術決策都應從「我們真正需要什麼資料？最直接的取得方式是什麼？」出發。

## 資料取得優先順序

嚴格按照以下順序評估資料來源：

1. **Yahoo Finance API** — 優先查看 Yahoo Finance 是否提供可串接的 API（如 `yfinance` Python 套件或公開 REST endpoint）。API 是最穩定、最乾淨的資料來源。
2. **官方網站爬蟲** — 若 Yahoo Finance 無法滿足需求（無 API、資料不完整、被封鎖等），再去爬取官方網站（如台灣證券交易所 TWSE、櫃買中心 TPEx）。

不要在未確認 API 可用性之前就直接寫爬蟲。

## 資料儲存

- 所有爬取結果一律存入 **SQLite** 資料庫檔案（`.db`）。
- 不使用 CSV、JSON 等純文字格式作為主要儲存方式。
- 資料表設計應包含時間戳記欄位，方便追蹤資料更新時間。

## 專案結構

```
上市櫃指數爬蟲/
├── CLAUDE.md               # 本文件：開發指南與專案說明
├── crawler.py              # 上市櫃指數爬蟲（TWSE + TPEx）
├── tx_futures_crawler.py   # 台指期貨爬蟲（TAIFEX）
├── strategy_signal.py      # 四票制策略訊號計算（每日自動執行）
├── signal_gui.py           # 策略訊號 Web Dashboard（本地 HTTP 伺服器）
├── open_dashboard.bat      # 一鍵開啟 Web Dashboard
├── daily_update.bat        # Windows 排程用批次檔（每日 17:03 自動執行）
├── stock_index.db          # SQLite 資料庫（上市櫃指數）
├── tx_futures.db           # SQLite 資料庫（台指期貨）
├── strategy_signal.db      # SQLite 資料庫（策略訊號與回測結果）
└── update_log.txt          # 每日排程執行的 log 輸出
```

## 資料庫 Schema（stock_index.db）

### twse_index / tpex_index

| 欄位 | 型別 | 說明 |
|---|---|---|
| date | TEXT PK | 日期（YYYY-MM-DD） |
| open | REAL | 開盤價 |
| high | REAL | 最高價 |
| low | REAL | 最低價 |
| close | REAL | 收盤價 |
| trading_volume | REAL | 成交量（股） |
| trading_value | REAL | 成交金額（元） |
| transactions | INTEGER | 成交筆數 |
| updated_at | TEXT | 資料更新時間（ISO format） |

### crawl_progress

| 欄位 | 型別 | 說明 |
|---|---|---|
| task | TEXT | 任務名稱（如 `twse_mi_index`） |
| date | TEXT | 已完成的日期 |

用於追蹤 TWSE Phase 2（MI_INDEX 逐日更新）的進度，使爬蟲可中斷後續跑。

## 資料來源 API

### TWSE 加權指數

| 階段 | API | 用途 | 備註 |
|---|---|---|---|
| Phase 1 | `MI_5MINS_HIST` | 每月 OHLC | 月批次，民國年日期 |
| Phase 1 | `FMTQIK` | 每月成交金額（暫存） | 全市場含 ETF/權證，2004+ 會被 Phase 2 覆蓋 |
| Phase 2 | `MI_INDEX` type=ALL | 逐日成交金額（修正） | tables[6]「股票合計」列，不含 ETF/權證，2004/02/11 起可用 |

- TWSE API base: `https://www.twse.com.tw/rwd/zh/`
- 日期格式：`YYYYMMDD`，回傳民國年
- stat 成功值：`"OK"`
- 限流：約 3 秒間隔，偶爾需重試

### TPEx 櫃買指數

| API | 用途 | 備註 |
|---|---|---|
| `indexInfo/inx` | 每月 OHLC | 西元日期 |
| `tradingIndexRpk` | 每月成交金額（2009+） | 含全部上櫃證券（含盤後/零股/鉅額），仟股/仟元需 ×1000 |
| `tradingIndex` | 每月成交金額（2000-2008 fallback） | 上櫃股票，仟股/仟元需 ×1000 |

- TPEx API base: `https://www.tpex.org.tw/www/zh-tw/`
- 日期格式：`YYYY/MM/DD`，OHLC 回傳西元、成交金額回傳民國年
- stat 成功值：`"ok"`（小寫）

## 爬蟲執行流程

### crawler.py（上市櫃指數）

1. **TWSE Phase 1**（月批次）：從 DB 最新日期所在月份開始，逐月抓 OHLC + FMTQIK
2. **TWSE Phase 2**（逐日）：對 2004/02/11 後尚未處理的日期，逐日呼叫 MI_INDEX 修正成交金額，進度記錄在 `crawl_progress` 表
3. **TPEx**（月批次）：從 DB 最新日期所在月份開始，逐月抓 OHLC + tradingIndexRpk（2009 前 fallback tradingIndex）

全程可中斷續跑：Phase 1/TPEx 依 DB 最新日期續接，Phase 2 依 crawl_progress 續接。

### tx_futures_crawler.py（台指期貨）

1. 查詢 DB 最新交易日期
2. 從最新日期**當天**開始（非 +1 天），按月批次 POST 請求 TAIFEX futDataDown API
3. 解析 big5 編碼 CSV，過濾價差單，INSERT OR REPLACE 到 tx_futures 表

從當天重抓而非 +1 天的原因：盤後交易時段（15:00~隔日05:00）資料會比一般盤更早入庫，
若用 +1 天判斷會誤認為已完成，導致一般盤資料漏抓。INSERT OR REPLACE 確保不重複。

可中斷續跑：依 DB 最新交易日期自動接續。

## 與 Yahoo Finance 的數值差異

以 2026-03-31 為基準：

| | 本爬蟲 | Yahoo Finance | 差異 | 原因 |
|---|---|---|---|---|
| TWSE | 7931.40億 | 7939.63億 | 0.1% | MI_INDEX 統計口徑微差 |
| TPEx | 2140.18億 | 2102.56億 | 1.8% | tradingIndexRpk 含盤後/零股/鉅額，Yahoo 不含 |

## 資料庫 Schema（tx_futures.db）

### tx_futures

| 欄位 | 型別 | 說明 |
|---|---|---|
| trading_date | TEXT PK | 交易日期（YYYY/MM/DD） |
| contract | TEXT | 契約（TX） |
| delivery_month | TEXT PK | 到期月份（如 202604） |
| open_price | INTEGER | 開盤價 |
| high_price | INTEGER | 最高價 |
| low_price | INTEGER | 最低價 |
| close_price | INTEGER | 收盤價 |
| price_change | INTEGER | 漲跌價 |
| change_percent | REAL | 漲跌%（含 % 符號的字串） |
| volume | INTEGER | 成交量 |
| settlement_price | INTEGER | 結算價（無則為 '-'） |
| open_interest | INTEGER | 未沖銷契約數（無則為 '-'） |
| session | TEXT PK | 交易時段（一般/盤後） |

PK = (trading_date, contract, delivery_month, session)

## 台指期貨爬蟲（tx_futures_crawler.py）

- 資料來源：台灣期貨交易所 (TAIFEX) `futDataDown` API（POST）
- 商品代碼：TX（臺股期貨）
- 按月批次下載 CSV（big5 編碼），過濾價差單（delivery_month 含 '/'）
- 可中斷續跑：依 DB 最新日期自動接續
- API 限流：每次請求間隔 3 秒，失敗自動重試 3 次

## 每日自動更新

透過 Windows Task Scheduler 排程，任務名稱 `StockIndexCrawler`，每日 17:03 執行 `daily_update.bat`。

```batch
# 查看排程
powershell "schtasks /query /tn StockIndexCrawler"

# 手動觸發
powershell "schtasks /run /tn StockIndexCrawler"

# 刪除排程
powershell "schtasks /delete /tn StockIndexCrawler /f"
```

## 四票制策略訊號（strategy_signal.py）

- 資料來源：本地 `tx_futures.db`（台指期近月日盤）+ `stock_index.db`（櫃買指數收盤價）
- 無外部 API 依賴（已移除 FinLab API）
- 計算四張投票 → 總票數 → 目標部位 → 回測損益
- 結果存入 `strategy_signal.db` 的 `daily_signals` 表
- 每日 17:03 由 `daily_update.bat` 自動執行（在爬蟲之後）

### 資料庫 Schema（strategy_signal.db）

#### daily_signals

| 欄位 | 型別 | 說明 |
|---|---|---|
| trading_date | TEXT | 交易日期（YYYY-MM-DD） |
| close_price | INTEGER | 收盤價 |
| vote1 ~ vote4 | INTEGER | 各策略投票（+1/-1/0） |
| total_votes | INTEGER | 總票數（-4 ~ +4） |
| tech_score | INTEGER | 技術分（0/1/2） |
| target_position | INTEGER | 目標部位（1=做多, -1=做空, 0=空手） |
| position | INTEGER | 實際持倉 |
| daily_pnl | REAL | 當日損益（點數） |
| cumulative_pnl | REAL | 累積損益 |
| updated_at | TEXT | 計算時間 |

## 策略訊號 Web Dashboard（signal_gui.py）

- 本地 HTTP 伺服器（Python 標準庫，零外部依賴）
- 暗色主題 Web 介面，瀏覽器開啟 http://127.0.0.1:8787
- 顯示：最新訊號 / 統計摘要 / 年度績效 / 近期交易記錄
- 「重新計算訊號」按鈕可即時重算（呼叫 strategy_signal.py）
- 啟動：雙擊 `open_dashboard.bat` 或 `python signal_gui.py`
- API: GET `/api/data` 取得 JSON、POST `/api/recalculate` 重算

## 注意事項

- Python 3.14 對 SSL 驗證較嚴格，需設定 `SESSION.verify = False` 才能連線 TWSE/TPEx
- Windows cp950 終端不支援 Unicode emoji，print 只用 ASCII 字元（如 `[WARN]`）
- TWSE API 偶爾限流回傳空內容，`api_get_json()` 內建最多 3 次重試（10s/20s/30s）
- TPEx 成交金額單位為仟股/仟元，程式內已乘以 1000 轉換
- `daily_update.bat` 使用 `%~dp0` 定位目錄，不可改為硬編碼中文路徑（Task Scheduler 的 cmd.exe 編碼會導致 `cd /d` 失敗）
