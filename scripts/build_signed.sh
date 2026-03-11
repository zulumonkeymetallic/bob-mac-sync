#!/usr/bin/env bash
set -euo pipefail

# Builds a signed Reminders MenuBar app so Firebase/Google Sign-In can use the keychain.
# Requires: Xcode command-line tools, a valid Apple Development certificate, and that
# you're signed into Xcode with the specified team ID.
#
# Build output location:
# - The Xcode post-build action exports the app to iCloud Drive:
#   /Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync/[timestamp]/
# - Only the .app bundle is exported (dSYM debug symbols are excluded)

SCHEME="${SCHEME:-Reminders MenuBar}"
CONFIGURATION="${CONFIGURATION:-Release}"
DESTINATION="${DESTINATION:-platform=macOS,arch=arm64}"
DERIVED_DATA="${DERIVED_DATA:-./build_out}"
TEAM_ID="${TEAM_ID:-J877BRYKD6}"
CODE_SIGN_IDENTITY="${CODE_SIGN_IDENTITY:-Apple Development}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Building ${SCHEME} (${CONFIGURATION})"
echo "Team ID: ${TEAM_ID}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Clean build folder and derived data
echo "Cleaning build folder and derived data..."
rm -rf "${DERIVED_DATA}"
xcodebuild clean \
  -scheme "${SCHEME}" \
  -configuration "${CONFIGURATION}" \
  -derivedDataPath "${DERIVED_DATA}" \
  -destination "${DESTINATION}" \
  -quiet 2>/dev/null || true
echo "Clean complete."
echo ""

# Build the app
xcodebuild \
  -scheme "${SCHEME}" \
  -configuration "${CONFIGURATION}" \
  -derivedDataPath "${DERIVED_DATA}" \
  -destination "${DESTINATION}" \
  DEVELOPMENT_TEAM="${TEAM_ID}" \
  CODE_SIGN_STYLE=Automatic \
  CODE_SIGN_IDENTITY="${CODE_SIGN_IDENTITY}" \
  CODE_SIGNING_ALLOWED=YES \
  CODE_SIGNING_REQUIRED=YES \
  DEBUG_INFORMATION_FORMAT=dwarf-with-dsym

BUILD_STATUS=$?

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ $BUILD_STATUS -eq 0 ]; then
    echo "✅ Build succeeded!"
    echo ""
    echo "The app has been exported to iCloud Drive:"
    echo "/Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync/[timestamp]/"
else
    echo "❌ Build failed with status ${BUILD_STATUS}"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

exit $BUILD_STATUS
