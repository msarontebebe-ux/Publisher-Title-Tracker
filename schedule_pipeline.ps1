# Schedule Springer Pipeline with Windows Task Scheduler
# Run this script as Administrator to set up automated scheduling

param(
    [string]$Schedule = "Daily",  # Daily, Weekly, Incremental
    [string]$Time = "02:00",       # Time to run (24-hour format)
    [string]$VenvPath = ".\.venv\Scripts\Activate.ps1"
)

Write-Host "=" -ForegroundColor Cyan
Write-Host "Springer Pipeline Scheduler Setup" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

# Get current directory
$ProjectPath = (Get-Location).Path
$OrchestratorScript = Join-Path $ProjectPath "orchestrator.py"
$LogPath = Join-Path $ProjectPath "logs"

# Validate orchestrator exists
if (-not (Test-Path $OrchestratorScript)) {
    Write-Host "ERROR: orchestrator.py not found in $ProjectPath" -ForegroundColor Red
    exit 1
}

# Create logs directory
if (-not (Test-Path $LogPath)) {
    New-Item -ItemType Directory -Path $LogPath | Out-Null
    Write-Host "Created logs directory: $LogPath" -ForegroundColor Green
}

# Create wrapper script that activates venv and runs orchestrator
$WrapperScriptPath = Join-Path $ProjectPath "run_scheduled_pipeline.ps1"
$WrapperContent = @"
# Scheduled pipeline execution wrapper
# This script is called by Task Scheduler

Set-Location "$ProjectPath"

# Activate virtual environment
try {
    & "$VenvPath"
} catch {
    Write-Host "Failed to activate virtual environment" -ForegroundColor Red
    exit 1
}

# Run orchestrator based on schedule type
switch ("$Schedule") {
    "Daily" {
        python orchestrator.py --year-start 2020
    }
    "Weekly" {
        python orchestrator.py --year-start 2015
    }
    "Incremental" {
        python orchestrator.py --incremental
    }
    default {
        python orchestrator.py --incremental
    }
}

# Capture exit code
`$exitCode = `$LASTEXITCODE
Write-Host "Pipeline completed with exit code: `$exitCode"
exit `$exitCode
"@

Set-Content -Path $WrapperScriptPath -Value $WrapperContent
Write-Host "Created wrapper script: $WrapperScriptPath" -ForegroundColor Green

# Task Scheduler configuration
$TaskName = "SpringerPipeline_$Schedule"
$Description = "Automated Springer publication data pipeline ($Schedule)"

# Check if task already exists
$ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($ExistingTask) {
    Write-Host ""
    Write-Host "Task '$TaskName' already exists!" -ForegroundColor Yellow
    $response = Read-Host "Do you want to replace it? (y/N)"
    if ($response -ne 'y' -and $response -ne 'Y') {
        Write-Host "Cancelled." -ForegroundColor Yellow
        exit 0
    }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task" -ForegroundColor Yellow
}

# Create scheduled task trigger based on schedule type
switch ($Schedule) {
    "Daily" {
        $Trigger = New-ScheduledTaskTrigger -Daily -At $Time
        Write-Host "Schedule: Daily at $Time" -ForegroundColor Cyan
    }
    "Weekly" {
        $Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $Time
        Write-Host "Schedule: Weekly (Sundays) at $Time" -ForegroundColor Cyan
    }
    "Incremental" {
        $Trigger = New-ScheduledTaskTrigger -Daily -At $Time
        Write-Host "Schedule: Daily (incremental) at $Time" -ForegroundColor Cyan
    }
    default {
        Write-Host "Invalid schedule type: $Schedule" -ForegroundColor Red
        Write-Host "Valid options: Daily, Weekly, Incremental" -ForegroundColor Yellow
        exit 1
    }
}

# Create action to run PowerShell script
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$WrapperScriptPath`""

# Task settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

# Register the scheduled task
try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $Description `
        -Trigger $Trigger `
        -Action $Action `
        -Settings $Settings `
        -User $env:USERNAME `
        -RunLevel Highest

    Write-Host ""
    Write-Host "SUCCESS! Scheduled task created: $TaskName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Cyan
    Write-Host "  Name: $TaskName"
    Write-Host "  Schedule: $Schedule at $Time"
    Write-Host "  Script: $WrapperScriptPath"
    Write-Host "  Logs: $LogPath"
    Write-Host ""
    Write-Host "To manage this task:" -ForegroundColor Yellow
    Write-Host "  View:   Get-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  Run:    Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  Remove: Unregister-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  Or use Task Scheduler GUI (taskschd.msc)"
    Write-Host ""
    
} catch {
    Write-Host "ERROR: Failed to create scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Offer to run a test
Write-Host "Would you like to run a test now? (y/N): " -ForegroundColor Yellow -NoNewline
$testResponse = Read-Host
if ($testResponse -eq 'y' -or $testResponse -eq 'Y') {
    Write-Host ""
    Write-Host "Running test execution..." -ForegroundColor Cyan
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "Task started. Check Task Scheduler for status and logs in $LogPath" -ForegroundColor Green
}
