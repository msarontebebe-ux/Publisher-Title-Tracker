# Scheduled pipeline execution wrapper
# This script is called by Task Scheduler.
# $Schedule comes from the task configuration: Daily | Weekly | Incremental

param(
    [string]$Schedule = "Incremental"
)

Set-Location "C:\Users\saron\project"

# Activate virtual environment
try {
    & ".\.venv\Scripts\Activate.ps1"
} catch {
    Write-Host "Failed to activate virtual environment" -ForegroundColor Red
    exit 1
}

# Run orchestrator based on schedule type
switch ($Schedule) {
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
$exitCode = $LASTEXITCODE
Write-Host "Pipeline completed with exit code: $exitCode"
exit $exitCode
