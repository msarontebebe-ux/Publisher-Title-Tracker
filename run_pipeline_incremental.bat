@echo off
REM Daily Incremental Pipeline Runner
cd /d C:\Users\saron\project
call .venv\Scripts\activate.bat
python orchestrator.py --incremental
