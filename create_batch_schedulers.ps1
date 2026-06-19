# Quick Setup for Scheduled Pipeline (No Admin Required Alternative)
# This creates a simple batch file you can use with Task Scheduler GUI

Write-Host "Creating simplified scheduler files..." -ForegroundColor Cyan

# Create wrapper batch file
$batchContent = @"
@echo off
cd /d C:\Users\saron\project
call .venv\Scripts\activate.bat
python orchestrator.py --incremental
"@

$batchPath = "run_pipeline_incremental.bat"
Set-Content -Path $batchPath -Value $batchContent

Write-Host "✓ Created: $batchPath" -ForegroundColor Green

# Create another for weekly
$batchContentWeekly = @"
@echo off
cd /d C:\Users\saron\project
call .venv\Scripts\activate.bat
python orchestrator.py --year-start 2020
"@

$batchPathWeekly = "run_pipeline_weekly.bat"
Set-Content -Path $batchPathWeekly -Value $batchContentWeekly

Write-Host "✓ Created: $batchPathWeekly" -ForegroundColor Green

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Open Task Scheduler: taskschd.msc" -ForegroundColor White
Write-Host "2. Action → Create Basic Task" -ForegroundColor White
Write-Host "3. Name: Springer Pipeline Incremental" -ForegroundColor White
Write-Host "4. Trigger: Daily at 2:00 AM" -ForegroundColor White
Write-Host "5. Action: Start a program" -ForegroundColor White
Write-Host "6. Program: $((Get-Location).Path)\$batchPath" -ForegroundColor White
Write-Host ""
Write-Host "Or test now by running:" -ForegroundColor Yellow
Write-Host "   .\$batchPath" -ForegroundColor Cyan
