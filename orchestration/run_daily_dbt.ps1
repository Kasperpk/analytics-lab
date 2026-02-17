$ErrorActionPreference = 'Stop'

# Resolve project paths relative to this script so Task Scheduler can run it from any working directory.
$root = Split-Path -Parent $PSScriptRoot
$dbtDir = Join-Path $root 'dbt/analytics_project'
$statusDir = Join-Path $root 'logs'
$statusFile = Join-Path $statusDir 'daily_dbt_status.json'

# Ensure status output folder exists before writing the run flag file.
if (-not (Test-Path $statusDir)) {
    New-Item -ItemType Directory -Path $statusDir | Out-Null
}

# Run dbt from the project directory so profile and relative model paths resolve correctly.
Push-Location $dbtDir
try {
    # Build includes model execution + dbt tests; a non-zero exit code means data quality or model failure.
    uv run dbt build --profiles-dir .
    $exitCode = $LASTEXITCODE

    # Persist a simple PASS/FAIL marker for monitoring or downstream alerting.
    $status = if ($exitCode -eq 0) { 'PASS' } else { 'FAIL' }

    # Emit machine-readable status metadata to support daily run observability.
    $payload = [ordered]@{
        timestamp_utc = (Get-Date).ToUniversalTime().ToString('o')
        command = 'uv run dbt build --profiles-dir .'
        status = $status
        exit_code = $exitCode
    }

    ($payload | ConvertTo-Json -Depth 3) | Set-Content -Encoding UTF8 $statusFile

    # Propagate failure to scheduler/orchestrator so the job run is marked failed.
    if ($exitCode -ne 0) {
        exit $exitCode
    }
}
finally {
    # Always restore previous location even if dbt fails.
    Pop-Location
}
