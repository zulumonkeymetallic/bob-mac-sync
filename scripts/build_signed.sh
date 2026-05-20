#!/usr/bin/env bash
set -euo pipefail

# Canonical local build script.
# It builds in a local staging directory, re-signs the app for the selected
# distribution mode, optionally notarizes, then exports the finished app into
# Google Drive (avoids iCloud file provider xattr injection that breaks codesign).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_FILE="${ROOT_DIR}/reminders-menubar.xcodeproj/project.pbxproj"
DEFAULT_EXPORT_ROOT="/Users/jim/Library/CloudStorage/GoogleDrive-Jdonnelly@jc1.tech/My Drive/BobMacSync"
DEFAULT_STAGE_ROOT="${HOME}/Library/Application Support/bobmacsync-builds"

detect_project_team() {
  sed -n 's/.*DEVELOPMENT_TEAM = \([A-Z0-9]*\);/\1/p' "${PROJECT_FILE}" | head -n 1
}

detect_identity() {
  local kind="$1"
  security find-identity -v -p codesigning 2>/dev/null \
    | sed -n "s/.*\"\\(${kind}: .* (\\([A-Z0-9]*\\))\\)\"/\\1|\\2/p" \
    | head -n 1
}

sanitize_entitlements() {
  local source_path="$1"
  local dest_path="$2"

  cp "${source_path}" "${dest_path}"
  /usr/libexec/PlistBuddy -c "Delete :com.apple.security.get-task-allow" "${dest_path}" >/dev/null 2>&1 || true
  # Firebase ships pre-built binary gRPC/Abseil frameworks whose bundle format
  # codesign treats as ambiguous. On macOS 26+ this causes hardened-runtime
  # library validation to refuse loading them under Developer ID. Disabling
  # library validation is the standard workaround for Firebase macOS apps.
  /usr/libexec/PlistBuddy -c "Add :com.apple.security.cs.disable-library-validation bool true" "${dest_path}" >/dev/null 2>&1 || \
  /usr/libexec/PlistBuddy -c "Set :com.apple.security.cs.disable-library-validation true" "${dest_path}" >/dev/null 2>&1 || true
}

wait_for_notarization() {
  local submission_id="$1"
  local poll_interval="${NOTARY_POLL_INTERVAL_SECONDS:-15}"

  while true; do
    local info_output
    info_output="$(xcrun notarytool info "${submission_id}" --keychain-profile "${NOTARY_PROFILE}")"
    local status
    status="$(printf '%s\n' "${info_output}" | sed -n 's/^  status: //p' | head -n 1)"

    echo "Notarization status: ${status:-unknown}"

    case "${status}" in
      Accepted)
        return 0
        ;;
      Invalid|Rejected)
        echo "Notarization failed; fetching log for ${submission_id}" >&2
        xcrun notarytool log "${submission_id}" --keychain-profile "${NOTARY_PROFILE}" || true
        return 1
        ;;
      "In Progress"|Submitted|"")
        sleep "${poll_interval}"
        ;;
      *)
        echo "Unexpected notarization status: ${status}" >&2
        sleep "${poll_interval}"
        ;;
    esac
  done
}

app_binary_hash() {
  local app_path="$1"
  local binary="${app_path}/Contents/MacOS/Reminders MenuBar"
  local launcher="${app_path}/Contents/Library/LoginItems/Reminders MenuBar Launcher.app/Contents/MacOS/Reminders MenuBar Launcher"
  local plist="${app_path}/Contents/Info.plist"
  # Hash main binary + login item binary + Info.plist so resource/plist-only
  # changes are not incorrectly treated as duplicates of a prior build.
  local inputs=()
  [[ -f "${binary}" ]] && inputs+=("${binary}")
  [[ -f "${launcher}" ]] && inputs+=("${launcher}")
  [[ -f "${plist}" ]] && inputs+=("${plist}")
  if [[ ${#inputs[@]} -gt 0 ]]; then
    shasum -a 256 "${inputs[@]}" | awk '{print $1}' | shasum -a 256 | awk '{print $1}'
  fi
}

find_duplicate_export() {
  local new_hash="$1"
  local export_root="$2"
  [[ -z "${new_hash}" ]] && return
  while IFS= read -r -d '' candidate; do
    local candidate_hash
    candidate_hash="$(app_binary_hash "${candidate}")"
    if [[ "${candidate_hash}" == "${new_hash}" ]]; then
      printf '%s\n' "$(dirname "${candidate}")"
      return
    fi
  done < <(find "${export_root}" -maxdepth 2 -name "Reminders MenuBar.app" -print0 2>/dev/null)
}

PROJECT_TEAM_ID="${PROJECT_TEAM_ID:-$(detect_project_team || true)}"
DETECTED_DEV_IDENTITY="${DETECTED_DEV_IDENTITY:-$(detect_identity 'Apple Development' || true)}"
DETECTED_DEVELOPER_ID_APP="${DETECTED_DEVELOPER_ID_APP:-$(detect_identity 'Developer ID Application' || true)}"

SCHEME="${SCHEME:-Reminders MenuBar}"
CONFIGURATION="${CONFIGURATION:-Release}"
DESTINATION="${DESTINATION:-platform=macOS,arch=arm64}"
TIMESTAMP="${TIMESTAMP:-$(date '+%d_%m_%y_%H_%M')}"
TEAM_ID="${TEAM_ID:-${PROJECT_TEAM_ID:-}}"
ALLOW_PROVISIONING_UPDATES="${ALLOW_PROVISIONING_UPDATES:-1}"
SIGNING_MODE="${SIGNING_MODE:-development}"
STAGE_ROOT="${STAGE_ROOT:-${DEFAULT_STAGE_ROOT}}"
EXPORT_ROOT="${EXPORT_ROOT:-${DEFAULT_EXPORT_ROOT}}"
STAGE_DIR="${STAGE_DIR:-${STAGE_ROOT}/${TIMESTAMP}}"
DERIVED_DATA="${DERIVED_DATA:-${STAGE_DIR}/DerivedData}"
EXPORT_DIR="${EXPORT_DIR:-${EXPORT_ROOT}/${TIMESTAMP}}"
LAUNCH_APP="${LAUNCH_APP:-0}"
NOTARIZE_APP="${NOTARIZE_APP:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"
NOTARY_PROFILE="${NOTARY_PROFILE:-bobmacsync-notary}"
NOTARY_KEY_PATH="${NOTARY_KEY_PATH:-/Users/jim/Downloads/AppConnect Key}"
NOTARY_KEY_ID="${NOTARY_KEY_ID:-}"
NOTARY_ISSUER_ID="${NOTARY_ISSUER_ID:-}"

STAGED_APP_PATH="${DERIVED_DATA}/Build/Products/${CONFIGURATION}/Reminders MenuBar.app"
STAGED_LOGIN_ITEM_PATH="${DERIVED_DATA}/Build/Products/${CONFIGURATION}/Reminders MenuBar Launcher.app"
DIST_APP_PATH="${STAGE_DIR}/Reminders MenuBar.app"
DIST_LOGIN_ITEM_PATH="${DIST_APP_PATH}/Contents/Library/LoginItems/Reminders MenuBar Launcher.app"
EXPORT_APP_PATH="${EXPORT_DIR}/Reminders MenuBar.app"
NOTARY_ZIP_PATH="${STAGE_DIR}/Reminders MenuBar.zip"
MAIN_XCENT="${DERIVED_DATA}/Build/Intermediates.noindex/reminders-menubar.build/${CONFIGURATION}/Reminders MenuBar.build/Reminders MenuBar.app.xcent"
LOGIN_ITEM_XCENT="${DERIVED_DATA}/Build/Intermediates.noindex/reminders-menubar.build/${CONFIGURATION}/Reminders MenuBar Launcher.build/Reminders MenuBar Launcher.app.xcent"
DIST_MAIN_XCENT="${STAGE_DIR}/Reminders MenuBar.dist.xcent"
DIST_LOGIN_ITEM_XCENT="${STAGE_DIR}/Reminders MenuBar Launcher.dist.xcent"

if [[ -z "${TEAM_ID}" ]]; then
  echo "No development team could be determined. Set TEAM_ID and rerun." >&2
  exit 1
fi

mkdir -p "${STAGE_DIR}" "${EXPORT_DIR}"

if [[ -n "${DETECTED_DEV_IDENTITY}" ]]; then
  echo "Detected Apple Development cert: ${DETECTED_DEV_IDENTITY%%|*}"
fi
if [[ -n "${DETECTED_DEVELOPER_ID_APP}" ]]; then
  echo "Detected Developer ID cert: ${DETECTED_DEVELOPER_ID_APP%%|*}"
fi

echo "Building ${SCHEME} (${CONFIGURATION})"
echo "Project team: ${PROJECT_TEAM_ID:-unknown}"
echo "Selected team: ${TEAM_ID}"
echo "Signing mode: ${SIGNING_MODE}"
echo "Local staging: ${STAGE_DIR}"
echo "Export directory: ${EXPORT_DIR}"

xcodebuild_args=(
  -project "${ROOT_DIR}/reminders-menubar.xcodeproj"
  -scheme "${SCHEME}"
  -configuration "${CONFIGURATION}"
  -derivedDataPath "${DERIVED_DATA}"
  -destination "${DESTINATION}"
  DEVELOPMENT_TEAM="${TEAM_ID}"
  CODE_SIGN_STYLE=Automatic
  CODE_SIGNING_ALLOWED=YES
  CODE_SIGNING_REQUIRED=YES
)

if [[ "${ALLOW_PROVISIONING_UPDATES}" == "1" ]]; then
  xcodebuild_args+=(-allowProvisioningUpdates)
fi

if [[ "${SKIP_BUILD}" != "1" ]]; then
  xcodebuild "${xcodebuild_args[@]}"
fi

if [[ ! -d "${STAGED_APP_PATH}" ]]; then
  echo "Built app not found at: ${STAGED_APP_PATH}" >&2
  exit 1
fi

echo "Verifying staged build"
codesign --verify --deep --strict --verbose=2 "${STAGED_APP_PATH}"

rm -rf "${DIST_APP_PATH}" "${NOTARY_ZIP_PATH}" "${EXPORT_APP_PATH}"
ditto "${STAGED_APP_PATH}" "${DIST_APP_PATH}"

case "${SIGNING_MODE}" in
  development)
    # Keep the Xcode-embedded provisioning profile and signature intact.
    # On macOS 26+ the system enforces that restricted entitlements (e.g.
    # mach-lookup temporary exceptions) are backed by a valid profile.
    echo "Development mode: using Xcode-signed build as-is (profile retained)"
    ;;
  developer-id)
    find "${DIST_APP_PATH}" -exec xattr -c {} ';' 2>/dev/null || true
    rm -f "${DIST_APP_PATH}/Contents/embedded.provisionprofile"
    rm -f "${DIST_LOGIN_ITEM_PATH}/Contents/embedded.provisionprofile"
    sanitize_entitlements "${MAIN_XCENT}" "${DIST_MAIN_XCENT}"
    sanitize_entitlements "${LOGIN_ITEM_XCENT}" "${DIST_LOGIN_ITEM_XCENT}"

    DIST_IDENTITY="${DETECTED_DEVELOPER_ID_APP%%|*}"
    if [[ -z "${DIST_IDENTITY}" ]]; then
      echo "Developer ID Application signing identity not found." >&2
      exit 1
    fi
    echo "Re-signing exported app with Developer ID Application"
    # --generate-entitlement-der is required on macOS 26+: the kernel enforces
    # that sandboxed Developer ID apps embed a DER-encoded entitlements blob in
    # their code signature; without it launchd refuses to spawn the process.
    codesign --force --sign "${DIST_IDENTITY}" --entitlements "${DIST_LOGIN_ITEM_XCENT}" --options runtime --timestamp --generate-entitlement-der "${DIST_LOGIN_ITEM_PATH}"
    codesign --force --deep --sign "${DIST_IDENTITY}" --entitlements "${DIST_MAIN_XCENT}" --options runtime --timestamp --generate-entitlement-der "${DIST_APP_PATH}"
    ;;
  *)
    echo "Unsupported SIGNING_MODE: ${SIGNING_MODE}" >&2
    exit 1
    ;;
esac

echo "Verifying staged distribution app"
codesign --verify --deep --strict --verbose=2 "${DIST_APP_PATH}"

if [[ "${SIGNING_MODE}" == "developer-id" && "${NOTARIZE_APP}" == "1" ]]; then
  # If the keychain profile already exists (xcrun notarytool history succeeds),
  # skip re-storing credentials — avoids requiring NOTARY_KEY_ID/ISSUER_ID on
  # every run. Only store when the profile is absent or explicitly overridden.
  profile_valid=0
  if xcrun notarytool history --keychain-profile "${NOTARY_PROFILE}" >/dev/null 2>&1; then
    profile_valid=1
    echo "Notary keychain profile '${NOTARY_PROFILE}' already valid — skipping store-credentials"
  fi

  if [[ "${profile_valid}" == "0" ]]; then
    if [[ -z "${NOTARY_KEY_ID}" || -z "${NOTARY_ISSUER_ID}" ]]; then
      echo "Notary keychain profile '${NOTARY_PROFILE}' not found." >&2
      echo "Set NOTARY_KEY_ID and NOTARY_ISSUER_ID to store credentials, or run:" >&2
      echo "  xcrun notarytool store-credentials ${NOTARY_PROFILE} --key <key.p8> --key-id <KEY_ID> --issuer <ISSUER_UUID>" >&2
      exit 1
    fi
    if [[ ! -f "${NOTARY_KEY_PATH}" ]]; then
      echo "Notary API key not found at: ${NOTARY_KEY_PATH}" >&2
      exit 1
    fi
    echo "Storing notarytool credentials"
    xcrun notarytool store-credentials "${NOTARY_PROFILE}" \
      --key "${NOTARY_KEY_PATH}" \
      --key-id "${NOTARY_KEY_ID}" \
      --issuer "${NOTARY_ISSUER_ID}" \
      --validate
  fi

  echo "Preparing notarization archive"
  ditto -c -k --keepParent "${DIST_APP_PATH}" "${NOTARY_ZIP_PATH}"

  echo "Submitting for notarization"
  submit_output="$(xcrun notarytool submit "${NOTARY_ZIP_PATH}" \
    --keychain-profile "${NOTARY_PROFILE}")"
  printf '%s\n' "${submit_output}"
  submission_id="$(printf '%s\n' "${submit_output}" | sed -n 's/^  id: //p' | head -n 1)"
  if [[ -z "${submission_id}" ]]; then
    echo "Unable to determine notarization submission ID." >&2
    exit 1
  fi

  echo "Waiting for notarization to complete"
  wait_for_notarization "${submission_id}"

  echo "Stapling notarization ticket"
  xcrun stapler staple -v "${DIST_APP_PATH}"

  echo "Assessing Gatekeeper acceptance"
  spctl -a -vv "${DIST_APP_PATH}"
fi

DIST_BINARY_HASH="$(app_binary_hash "${DIST_APP_PATH}")"
DUPLICATE_EXPORT="$(find_duplicate_export "${DIST_BINARY_HASH}" "${EXPORT_ROOT}")"
if [[ -n "${DUPLICATE_EXPORT}" ]]; then
  echo "Duplicate detected — identical binary already exported to: ${DUPLICATE_EXPORT}"
  echo "Skipping export. Set TIMESTAMP manually to force a new export directory."
  EXPORT_APP_PATH="${DUPLICATE_EXPORT}/Reminders MenuBar.app"
else
  echo "Exporting final app to Google Drive"
  ditto "${DIST_APP_PATH}" "${EXPORT_APP_PATH}"
fi

# Strip any xattrs that accumulate during copy (belt-and-suspenders).
find "${EXPORT_APP_PATH}" -exec xattr -c {} ';' 2>/dev/null || true

echo "Verifying exported app"
codesign --verify --deep --verbose=2 "${EXPORT_APP_PATH}"
if [[ "${SIGNING_MODE}" == "developer-id" && "${NOTARIZE_APP}" == "1" ]]; then
  spctl -a -vv "${EXPORT_APP_PATH}"
fi

if [[ "${LAUNCH_APP}" == "1" ]]; then
  echo "Launching ${EXPORT_APP_PATH}"
  open "${EXPORT_APP_PATH}"
fi

# Keep DerivedData only for the current build — older ones accumulate quickly.
ls -dt "${STAGE_ROOT}"/*/  2>/dev/null | tail -n +2 | while read old_dir; do
  [[ -d "${old_dir}DerivedData" ]] && rm -rf "${old_dir}DerivedData" && echo "Cleaned DerivedData: ${old_dir}"
done || true
