@echo off
setlocal
cd /d "C:\Users\engli\OneDrive\桌面\上市櫃指數爬蟲"

echo [%date% %time%] Starting sync... >> sync_log.txt

REM Fetch latest from remote
git fetch origin master >> sync_log.txt 2>&1

REM Extract the latest db files directly (bypasses OneDrive file lock)
git show origin/master:stock_index.db > stock_index.db 2>> sync_log.txt
git show origin/master:tx_futures.db > tx_futures.db 2>> sync_log.txt

REM Fast-forward the branch pointer to match remote
git reset origin/master >> sync_log.txt 2>&1

echo [%date% %time%] Sync done. >> sync_log.txt
