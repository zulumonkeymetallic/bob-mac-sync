#!/bin/zsh
set -euo pipefail

target_ref="${1:-TK-O50IP6}"
log_dir="${LOG_DIR:-/Users/jim/Library/Containers/com.jc1.tech.bob.mac/Data/Library/Logs/RemindersMenuBar}"

if [[ ! -d "$log_dir" ]]; then
  echo "Log directory not found: $log_dir" >&2
  exit 1
fi

latest_log="$(ls -1t "$log_dir"/sync*.log 2>/dev/null | head -n 1)"

if [[ -z "$latest_log" ]]; then
  echo "No sync logs found under $log_dir" >&2
  exit 1
fi

echo "Target ref: $target_ref"
echo "Latest log: $latest_log"
echo

echo "=== Recent sync logs ==="
ls -1t "$log_dir"/sync*.log | head -n 10
echo

echo "=== Matches for target ref and sync diagnostics ==="
grep -nE "$target_ref|staleTop3Reconciled|mergeReminder|updateFromReminder|previousMacSyncedAt|previousServerUpdatedAt|aiTop3Date|aiPriorityRank|macSyncedAt|serverUpdatedAt" "$latest_log" | tail -n 120 || true
echo

echo "=== Focused branch/timestamp diagnostics ==="
grep -nE "branch=mergeReminder|action: \"mergeReminder\"|action: \"updateFromReminder\"|previousMacSyncedAt|previousServerUpdatedAt|staleTop3Reconciled" "$latest_log" | tail -n 120 || true
echo

echo "=== Suggested next checks ==="
echo "1. Run one full sync from Xcode using the Reminders MenuBar scheme."
echo "2. Re-run: ./scripts/validate_sync_trace.sh $target_ref"
echo "3. Confirm the log shows either mergeReminder or updateFromReminder for the target item."
echo "4. Confirm staleTop3Reconciled=true when aiTop3Date was stale."
echo "5. Confirm previousMacSyncedAt/previousServerUpdatedAt appear in diagnostics."