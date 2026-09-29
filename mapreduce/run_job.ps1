# mapreduce/run_job.ps1 - PowerShell script for MapReduce job execution
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "BDA-MH-K5: Executing MapReduce Pipeline (Windows PowerShell)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

python "$ScriptDir\run_mapreduce.py"

Write-Host "MapReduce Pipeline Completed." -ForegroundColor Green
