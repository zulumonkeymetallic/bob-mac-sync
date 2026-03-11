#!/usr/bin/env bash
# Convenience script to manage the 7-day automated build system

set -euo pipefail

AGENT_LABEL="com.bob.reminders-menubar.build-deploy"
AGENT_PATH="/Users/jim/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist"
DEPLOY_SCRIPT="/Users/jim/GitHub/bob-mac-sync/scripts/build_and_deploy.sh"
LOG_DIR="/Users/jim/.logs"

command="${1:-help}"

case "$command" in
    start|enable)
        echo "🔄 Starting automated builds..."
        launchctl load -w "$AGENT_PATH" 2>/dev/null || true
        sleep 1
        if launchctl list | grep -q "$AGENT_LABEL"; then
            echo "✅ Automated builds enabled (runs every 7 days)"
        else
            echo "❌ Failed to enable"
            exit 1
        fi
        ;;

    stop|disable)
        echo "⏹️  Stopping automated builds..."
        launchctl unload "$AGENT_PATH" 2>/dev/null || true
        sleep 1
        if ! launchctl list | grep -q "$AGENT_LABEL"; then
            echo "✅ Automated builds disabled"
        else
            echo "⚠️  Still loaded, you may need to manually unload"
        fi
        ;;

    now|force|trigger)
        echo "🚀 Triggering build now (don't wait 7 days)..."
        bash "$DEPLOY_SCRIPT"
        ;;

    status)
        echo "📊 Automated Build Status:"
        echo ""
        if launchctl list | grep -q "$AGENT_LABEL"; then
            echo "  Status: ✅ ENABLED (runs every 7 days)"
        else
            echo "  Status: ⏹️  DISABLED"
        fi
        echo ""
        echo "  Agent: $AGENT_LABEL"
        echo "  Script: $DEPLOY_SCRIPT"
        echo ""
        ;;

    logs)
        echo "📋 Build Logs:"
        echo ""
        if [ -f "$LOG_DIR/bob-build-deploy.stdout" ]; then
            echo "=== STDOUT ==="
            tail -20 "$LOG_DIR/bob-build-deploy.stdout"
        else
            echo "No stdout log yet"
        fi
        echo ""
        if [ -f "$LOG_DIR/bob-build-deploy.stderr" ]; then
            echo "=== STDERR ==="
            tail -20 "$LOG_DIR/bob-build-deploy.stderr"
        else
            echo "No stderr log yet"
        fi
        echo ""
        echo "Full deployment log:"
        if [ -f "/Users/jim/GitHub/bob-mac-sync/.logs/deploy.log" ]; then
            tail -30 "/Users/jim/GitHub/bob-mac-sync/.logs/deploy.log"
        else
            echo "Deploy log not found yet (will be created on first run)"
        fi
        ;;

    reload)
        echo "🔄 Reloading LaunchAgent..."
        launchctl unload "$AGENT_PATH" 2>/dev/null || true
        sleep 1
        launchctl load -w "$AGENT_PATH" 2>/dev/null || true
        sleep 1
        if launchctl list | grep -q "$AGENT_LABEL"; then
            echo "✅ LaunchAgent reloaded"
        else
            echo "❌ Failed to reload"
            exit 1
        fi
        ;;

    help)
        cat << EOF
🔧 BOB Reminders MenuBar - Automated Build Manager

Usage: $0 <command>

Commands:
  start       Enable automated 7-day builds
  stop        Disable automated builds
  status      Show current status
  now         Trigger a build immediately (don't wait 7 days)
  logs        Show recent build logs
  reload      Unload and reload the LaunchAgent
  help        Show this message

Examples:
  $0 status              # Check if builds are running
  $0 now                 # Build and deploy right now
  $0 logs                # See what happened in previous builds
  $0 stop                # Temporarily disable

Info:
  ✓ Builds run every 7 days automatically
  ✓ App installed to: /Applications/BOB-SyncService
  ✓ Logs stored in: ~/.logs/
  ✓ Old builds auto-cleaned to prevent signing conflicts
EOF
        ;;

    *)
        echo "❌ Unknown command: $command"
        echo "Run '$0 help' for usage"
        exit 1
        ;;
esac
