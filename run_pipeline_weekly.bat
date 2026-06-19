@echo off
REM Weekly Full Pipeline Runner
cd /d C:\Users\saron\project
call .venv\Scripts\activate.bat
python orchestrator.py --year-start 2020
