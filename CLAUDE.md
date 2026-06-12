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
├── institutional_crawler.py # 三大法人 + P/C ratio 爬蟲（TAIFEX / FinMind）
├── strategy_signal.py      # 四票制策略訊號計算（每日自動執行）
├── strategy_signal_v2.py   # v2 誠實版訊號：PIT 查表 + SMA200 趨勢（與 v1 並行）
├── signal_ledger.py        # append-only 訊號帳本（前瞻紙上實盤證據層；--report 看評估）
├── risk_layer_research.py  # 部位/風險層研究（2026-06-12 封存：全部未過採納規則）
├── us_market_crawler.py    # S&P 500 日線爬蟲（Yahoo chart API；每日排程 + 類比工具使用）
├── us_market.db            # SQLite（^GSPC 日線；不入版控—本機/雲端各自爬、3 秒可重建；
│                           #   曾因 OneDrive 檔案鎖卡死 git reset，故移出 git 同步路徑）
├── signal_gui.py           # 策略訊號 Web Dashboard（本地 HTTP 伺服器）
├── open_dashboard.bat      # 一鍵開啟 Web Dashboard
├── market_analogue.py      # 市場狀態類比引擎（描述性工具，非交易訊號）
├── analogue_gui.py         # 市場狀態類比 Web GUI（port 8788）
├── open_analogue.bat       # 一鍵開啟市場狀態類比工具
├── daily_update.bat        # Windows 排程用批次檔（每日 17:03 自動執行）
├── stock_index.db          # SQLite 資料庫（上市櫃指數）
├── tx_futures.db           # SQLite 資料庫（台指期貨）
├── institutional.db        # SQLite 資料庫（三大法人淨部位、TXO P/C ratio）
├── strategy_signal.db      # SQLite 資料庫（策略訊號與回測結果；含 daily_signals_v2）
├── signal_ledger.db        # SQLite 資料庫（append-only 帳本；本機+雲端雙寫，先寫者留存）
├── docs/research/          # 六輪因子研究 + 風險層研究的永久輸出記錄（見 SUMMARY.md）
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

## 三大法人與 P/C ratio 爬蟲（institutional_crawler.py）

存入 `institutional.db`，日期一律 `YYYY-MM-DD`：

### fut_institutional（台指期三大法人，PK = (date, identity)）

| 欄位 | 說明 |
|---|---|
| identity | `dealer`（自營商）/ `trust`（投信）/ `foreign`（外資） |
| long_vol / short_vol / net_vol | 多方/空方/淨 交易口數 |
| long_oi / short_oi / net_oi | 多方/空方/淨 未平倉口數 |

- 歷史回補：FinMind API `TaiwanFuturesInstitutionalInvestors`（data_id=TX，**2018-06 起**，數值與 TAIFEX 完全一致）
- 每日增量：TAIFEX `futContractsDateDown`（POST，commodityId=`TXF`）— **僅提供最近 3 年（滾動視窗）**，更早會回 HTML 錯誤頁

### pc_ratio（TXO 買賣權比，PK = date）

put_vol / call_vol / pc_vol_ratio / put_oi / call_oi / pc_oi_ratio

- TAIFEX `pcRatioDown`（POST）— 完整歷史 **2007/07/02 起**，單次查詢上限約 **1 個月**
- 回傳 CSV 為日期**降序**、行尾多一個逗號

### spot_flows（TWSE 市場三大法人現貨買賣超，PK = date）

foreign_net / trust_net / dealer_net / total_net（NT$，買進金額 − 賣出金額）

- 歷史回補：FinMind `TaiwanStockTotalInstitutionalInvestors`（2007 起）
- 每日增量：TWSE `rwd/zh/fund/BFI82U?type=day&dayDate=YYYYMMDD`（需 verify=False）
- 兩來源數值完全一致（已驗證）

### margin_totals（TWSE 市場融資融券餘額，PK = date）

margin_lots（融資餘額，交易單位）/ short_lots（融券）/ margin_money（融資金額仟元，FinMind 來源可能為空）

- 歷史回補：FinMind `TaiwanStockTotalMarginPurchaseShortSale`（2001 起）
- 每日增量：TWSE `rwd/zh/marginTrading/MI_MARGN?selectType=MS`
- 注意：MI_MARGN 約 21:00 公布，17:03 排程抓到的是前一日（訊號本來就用 t-1，無影響）

### large_trader_tx（TAIFEX 台指期大額交易人，PK = date）

t5/t10 × all/spec（全部大額交易人/特定法人）× long/short 未平倉口數 + mkt_oi

- TAIFEX `largeTraderFutDown`（POST）— **完整歷史 2004 起，無 3 年限制**（與 futContractsDateDown 不同！）
- 回傳含全部商品：程式僅保留 `商品=TX` 且 `月份=999999`（全契約合計）的列
- 注意：TX 大額交易人統計口徑為 TX+MTX/4+TMF/20 合併

### TAIFEX 限流注意（重要教訓）

- 連續快速請求（3 秒間隔）會觸發 IP 級封鎖，所有端點一起回 HTML 錯誤頁；停止後約 1 分鐘解除
- 請求間隔至少 **6 秒**；收到 HTML 回應應視為「被限流」→ 長退避重試（60/120/300s），**不可當成無資料跳過**

## 策略訊號 v2（strategy_signal_v2.py）

- 與 v1 並行執行，互不影響；結果存 `strategy_signal.db` 的 `daily_signals_v2` 表
- 修正 v1 兩個問題：(1) 票1/2/3 查表改 point-in-time expanding（標籤滯後 2 天、自 2001 累積）→ 回測無前視、歷史票不重繪；(2) 修正暖身期 notna() 失效 bug（v1 的 notna 檢查作用在 np.where 字串上恆為 True）
- 另含 SMA200 趨勢艙（多/空手，用換月調整連續價格）— 因子研究中唯一通過 2001-2019 與 2020-2026 兩期間一致性的訊號（風控用，非 alpha）
- 另含 **LT 紙上追蹤艙**（大額交易人特定法人淨部位 vs 20 日均，多/空跟隨）：與趨勢/四票相關性 ≈0~負、dev(2005-2019) Sharpe 0.54、test Sharpe 0.53（穩定），但 MDD 厚尾（test −8,678）；組合 C3 因事前承諾的 MDD 條款 dev 未過 → **不部署，僅紙上追蹤**
- ⚠️ LT 的 2020-2026 測試窗已於 2026-06-11 因設計失誤揭露（v2 回填歷史欄位時印出）。**未來任何 LT/組合採納決策，只能依據 2026-06-12 之後累積的紙上實盤記錄**（預先承諾的採納規則：滿 12 個月後，若實盤 LT Sharpe≥0.3 且與趨勢艙相關性≤0.2，方可考慮以半口規模納入）
- 因子研究完整紀錄（六輪 68 組，2026-06 封槍，詳見各檔頭部的事前承諾）：`factor_research.py`（趨勢/跳空/價差等 22 組）、`factor_research2.py`（傳統技術指標 16 組）、`factor_research3.py`（P/C + 三大法人 12 組）、`factor_research4.py`（日曆效應+互補組合 7 組）、`factor_research5.py`（外資現貨/大額交易人/融資 8 組）、`factor_research6.py`（LT 互補組合 3 組）；`bias_check*.py` 為 v1 前視偏差驗證腳本
- **全部輸出與結論已存永久記錄 `docs/research/`（總結見 `SUMMARY.md`）**：73 組無一通過事前門檻；兩個「差點過」者（BASIS dev 0.69、PCVOL dev 0.64）OOS 全滅。2020-2026 測試窗對所有在場策略已實質燒毀 → 此後唯一誠實的驗證貨幣是前瞻實盤記錄
- **修約 #1（2026-06-12，使用者授權）**：第 7 輪美股領先 4 組（`factor_research7.py`，門檻加嚴 Sh>=0.8）全數 dev 未過、test 未開，**重新封槍**。描述性發現：跟隨美股隔夜方向當沖台指 dev Sharpe −1.66（t=−7.2）— 台指開盤對美股訊息過度反應、日內均值回歸，但反向毛利蓋不過成本，不構成可交易訊號
- **修約 #2（2026-06-12，使用者授權）**：第 8 輪對類比工具引擎 A 的 32 模式做 PIT 查表回測（`factor_research8.py`，零自由參數）。dev Sharpe 0.18（< 永遠做多 0.53）、test 未開，**重新封槍**。結論：5 個 regime 維度的交互作用無可交易訊息，類比工具維持純描述性定位

## 訊號帳本（signal_ledger.py / signal_ledger.db）

- **目的**：`daily_signals_v2` 每天整表重算重寫，歷史可被改寫；帳本只 `INSERT OR IGNORE`，
  同一 (signal_date, sleeve) 寫入後永不更動，搭配每日 git commit 形成可稽核時間戳鏈
- 每日 17:03 由 `daily_update.bat`（本機）與 GitHub Actions（雲端，cron 同為 17:03 台灣時間）
  在 v2 之後各自執行 capture，記錄四艙 target：`votes_v1` / `votes_v2` / `trend` / `lt`
- **雙寫設計（2026-06-12 改）**：INSERT OR IGNORE 冪等 → 同一 (signal_date, sleeve) 先寫者留存、
  後寫者跳過；git 為同步媒介、captured_at 標示來源。理由：本機輸掉 push 競賽時會 reset 掉
  當日帳本列，雲端同寫可補洞；電腦未開機時雲端獨立成錄
- **乾淨窗起點 2026-06-12（寫死）**；LT 採納資格判定日 **2027-06-12**（滿 12 個月，
  門檻：實盤 LT Sharpe >= 0.3 且與趨勢艙日損益相關 <= 0.2 → 方可考慮半口納入）
- 評估：`python signal_ledger.py --report`（會計規則寫死：次日開盤執行、結算強平、1.5 點/邊、漏記日沿用前值）
- ⚠ 不可對帳本做 UPDATE/DELETE/REPLACE；不可改 CLEAN_START/GATE_DATE

## 部位/風險層研究（risk_layer_research.py，2026-06-12 封存）

- 封槍承諾涵蓋「日線因子」；本研究只動部位大小/執行會計，事前承諾見檔頭，輸出在 `docs/research/risk_layer_output.txt`
- 結論：(1) 波動率目標化三艙與組合**全部未過採納規則**（dev Sharpe 增益 < +0.10）→ 維持各艙 1 口；
  (2) 結算日換月 vs 強平：三艙方向不一致 → 維持強平；
  (3) 夜盤執行：趨勢艙等待成本 2020+ 每單位 +27.6 點（描述性、樣本少）→ 未採納，留待前瞻記錄判定（夜/早盤兩種執行價皆可由帳本 target + tx_futures.db 事後重建）

## 每日自動更新

雙軌自動更新，互為備援（2026-06-12 起雲端與本機完整對等）：

1. **本機**：Windows Task Scheduler，任務名稱 `StockIndexCrawler`，每日 17:03 執行 `daily_update.bat`
2. **雲端**：GitHub Actions `daily_update.yml`，cron 週一~五 09:03 UTC（=17:03 台灣，實際常延遲數分鐘到數小時），
   跑完整鏈路：全部爬蟲（含三大法人/SPX，限流失敗不擋後續）→ v1/v2 訊號 → 帳本 capture → commit/push
   電腦未開機時資料與帳本由雲端獨立補齊；兩邊都寫的日子由 INSERT OR REPLACE（資料）/
   INSERT OR IGNORE（帳本）保證冪等，push 競賽輸家自動棄單（bat 內建 reset-and-skip）

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

## 市場狀態類比工具（market_analogue.py / analogue_gui.py）

- **描述性工具，非交易訊號**：回答「今天的狀態像歷史上哪些時期、那些時期之後發生什麼」
- **10 個事前固定狀態維度**（價格 5：趨勢 SMA200★/動能60★/動能20/波動水準★/位階★；
  籌碼 3：LT★/外資現貨z/P-C未平倉比；外部 2：SPX趨勢/櫃買相對強弱）
- 2026-06-12 由 13 維限縮至 10 維（依概念冗餘度事前判斷，未看績效）：刪 RSI14（與動能20
  重複）、波動方向（二階量）、融資動向（機械跟隨價格）——取捨記錄見 market_analogue.py 檔頭
- 雙引擎：**A 粗分類模式** = ★核心 5 維 → 32 模式 + 波段切分（離散維度多了樣本密度
  指數下降，故僅核心 5 維；2004-08 起約 5,400 日）；**B 全因子相似日** = 10 維連續值
  全樣本 z、等權歐氏距離、top 30、類比日間隔 ≥10 交易日（2008-07 起約 4,400 日池）
- 統計一律附 n、95% 信賴區間、無條件基準對照；狀態定義與事後統計同源 →
  屬樣本內敘述統計，多重比較與波段群聚使證據力有限（GUI 內建警語區塊，不可移除）
- 與 signal_ledger / 部署路徑**完全隔離**；不接部位、未接入每日排程（GUI 內即時重算）
- 引擎 A 模式的預測力已於 R8（修約 #2）誠實檢驗：PIT 查表 dev Sharpe 0.18、未過門檻 →
  「模式」確定僅供描述，不得再以任何形式接上部位邏輯
- 啟動：雙擊 `open_analogue.bat` 或 `python analogue_gui.py` → http://127.0.0.1:8788
- API：GET `/api/analogue` 取得 JSON（每次呼叫即時重讀資料庫計算）
- us_market.db（SPX）已接入 daily_update.bat 每日更新（單一請求，失敗時美股維度
  以 ffill 容忍滯後，工具不中斷）

## 注意事項

- Python 3.14 對 SSL 驗證較嚴格，需設定 `SESSION.verify = False` 才能連線 TWSE/TPEx
- Windows cp950 終端不支援 Unicode emoji，print 只用 ASCII 字元（如 `[WARN]`）
- TWSE API 偶爾限流回傳空內容，`api_get_json()` 內建最多 3 次重試（10s/20s/30s）
- TPEx 成交金額單位為仟股/仟元，程式內已乘以 1000 轉換
- `daily_update.bat` 使用 `%~dp0` 定位目錄，不可改為硬編碼中文路徑（Task Scheduler 的 cmd.exe 編碼會導致 `cd /d` 失敗）
