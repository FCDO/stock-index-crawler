@echo off
cd /d "%~dp0"

echo === Daily Update Start: %DATE% %TIME% === >> update_log.txt 2>&1

REM === Step 1: Sync with remote (accept cloud's tip as canonical starting point) ===
git fetch origin >> update_log.txt 2>&1
git reset --hard origin/master >> update_log.txt 2>&1

REM === Step 2: Run crawlers and signal computation ===
C:\Python314\python.exe crawler.py >> update_log.txt 2>&1
C:\Python314\python.exe tx_futures_crawler.py >> update_log.txt 2>&1
C:\Python314\python.exe strategy_signal.py >> update_log.txt 2>&1
C:\Python314\python.exe export_signals.py >> update_log.txt 2>&1

REM === Step 3: Stage data files only (avoid committing notebooks/csv/png) ===
for /f "delims=" %%i in ('C:\Python314\python.exe -c "import datetime;print(datetime.date.today().isoformat())"') do set TODAY=%%i
git add stock_index.db tx_futures.db strategy_signal.db docs/signals.json >> update_log.txt 2>&1

REM === Step 4: Commit if there are staged changes ===
git diff --staged --quiet
if errorlevel 1 (
    git commit -m "Daily update: %TODAY% (local)" >> update_log.txt 2>&1

    REM === Step 5: Push, with reset-and-skip if cloud raced ahead ===
    git push >> update_log.txt 2>&1
    if errorlevel 1 (
        echo [WARN] Push lost race with cloud, resetting to origin/master >> update_log.txt 2>&1
        git fetch origin >> update_log.txt 2>&1
        git reset --hard origin/master >> update_log.txt 2>&1
    )
) else (
    echo [INFO] No data changes to commit >> update_log.txt 2>&1
)

echo === Daily Update End: %DATE% %TIME% === >> update_log.txt 2>&1
