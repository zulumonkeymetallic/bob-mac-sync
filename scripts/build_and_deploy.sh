#!/usr/bin/env bash
# Automatic signed build & deployment script
# Runs every 7 days via LaunchAgent, rebuilds the app, and replaces the version in /Applications
# Handles the 7-day code signing certificate cycle automatically

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS_DIR="${REPO_DIR}/scripts"
BUILD_SIGNED_SCRIPT="${SCRIPTS_DIR}/build_signed.sh"
DEPLOY_LOG_FILE="${REPO_DIR}/.logs/deploy.log"
DEPLOY_LOG_DIR="$(dirname "${DEPLOY_LOG_FILE}")"
APP_INSTALL_PATH="/Applications/BOB-SyncService"
ICLOUD_DRIVE_BASE="/Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync"

# Setup logging directory
mkdir -p "${DEPLOY_LOG_DIR}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${DEPLOY_LOG_FILE}"
}

log "═══════════════════════════════════════════════════════════"
log "Starting 7-day automated build & deploy cycle"
log "═══════════════════════════════════════════════════════════"

# Step 1: Clean old builds (7-day signing workaround)
log "Step 1: Removing builds older than 7 days to prevent signing conflicts..."
if [ -d "${ICLOUD_DRIVE_BASE}" ]; then
    find "${ICLOUD_DRIVE_BASE}" -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null || true
    log "  ✓ Cleaned old builds"
else
    log "  ⚠ iCloud Drive directory not found, skipping cleanup"
fi

# Step 2: Build the app
log "Step 2: Building signed app..."
if ! bash "${BUILD_SIGNED_SCRIPT}" >> "${DEPLOY_LOG_FILE}" 2>&1; then
    log "❌ Build failed"
    exit 1
fi
log "  ✓ Build succeeded"

# Step 3: Find the latest export
log "Step 3: Locating latest build..."
LATEST_BUILD=$(find "${ICLOUD_DRIVE_BASE}" -maxdepth 2 -name "Reminders MenuBar.app" -type d 2>/dev/null | sort -r | head -1)

if [ -z "${LATEST_BUILD}" ]; then
    log "❌ Could not find latest build"
    exit 1
fi

log "  Found: ${LATEST_BUILD}"

# Step 4: Deploy to /Applications
log "Step 4: Deploying to ${APP_INSTALL_PATH}..."

# Remove old version if it exists
if [ -d "${APP_INSTALL_PATH}" ]; then
    log "  Removing old version..."
    rm -rf "${APP_INSTALL_PATH}"
fi

# Copy new version
mkdir -p "$(dirname "${APP_INSTALL_PATH}")"
cp -r "${LATEST_BUILD}" "${APP_INSTALL_PATH}"
log "  ✓ Deployed to ${APP_INSTALL_PATH}"

# Step 5: Verify installation
log "Step 5: Verifying installation..."
if [ -d "${APP_INSTALL_PATH}" ]; then
    APP_SIZE=$(du -sh "${APP_INSTALL_PATH}" | cut -f1)
    log "  ✓ Verified: ${APP_SIZE}"
else
    log "❌ Verification failed"
    exit 1
fi

log "═══════════════════════════════════════════════════════════"
log "✅ Build & deploy cycle completed successfully!"
log "   App installed to: ${APP_INSTALL_PATH}"
log "═══════════════════════════════════════════════════════════"
