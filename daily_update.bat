@echo off
cd /d "%~dp0"
C:\Python314\python.exe crawler.py >> update_log.txt 2>&1
C:\Python314\python.exe tx_futures_crawler.py >> update_log.txt 2>&1
C:\Python314\python.exe strategy_signal.py >> update_log.txt 2>&1
