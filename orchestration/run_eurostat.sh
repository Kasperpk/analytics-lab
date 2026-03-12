#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# run_eurostat.sh — cross-platform wrapper for scheduled Eurostat refreshes
#
# Usage (manual):
#   bash orchestration/run_eurostat.sh
#
# Schedule with cron (e.g., every Monday at 06:00):
#   0 6 * * 1  cd /path/to/analytics-lab && bash orchestration/run_eurostat.sh
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STATUS_DIR="$ROOT/logs"
STATUS_FILE="$STATUS_DIR/eurostat_status.json"

mkdir -p "$STATUS_DIR"

cd "$ROOT"

echo "── Eurostat pipeline refresh: $(date -u +%Y-%m-%dT%H:%M:%SZ) ──"

if uv run pipeline eurostat; then
    STATUS="PASS"
    EXIT_CODE=0
else
    STATUS="FAIL"
    EXIT_CODE=$?
fi

# Write machine-readable status for monitoring / alerting
cat > "$STATUS_FILE" <<EOF
{
  "timestamp_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "command": "uv run pipeline eurostat",
  "status": "$STATUS",
  "exit_code": $EXIT_CODE
}
EOF

echo "── Status: $STATUS (logged to $STATUS_FILE) ──"
exit $EXIT_CODE
