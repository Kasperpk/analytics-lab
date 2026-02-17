$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$dbtDir = Join-Path $root 'dbt/analytics_project'
$statusDir = Join-Path $root 'logs'
$statusFile = Join-Path $statusDir 'daily_dbt_status.json'

if (-not (Test-Path $statusDir)) {
    New-Item -ItemType Directory -Path $statusDir | Out-Null
}

Push-Location $dbtDir
try {
    uv run dbt build --profiles-dir .
    $exitCode = $LASTEXITCODE

    $status = if ($exitCode -eq 0) { 'PASS' } else { 'FAIL' }

    $payload = [ordered]@{
        timestamp_utc = (Get-Date).ToUniversalTime().ToString('o')
        command = 'uv run dbt build --profiles-dir .'
        status = $status
        exit_code = $exitCode
    }

    ($payload | ConvertTo-Json -Depth 3) | Set-Content -Encoding UTF8 $statusFile

    if ($exitCode -ne 0) {
        exit $exitCode
    }
}
finally {
    Pop-Location
}
