BOB Mac Sync — Agents Guide

## Multi-Repo Build System

**This repository is part of the BOB multi-platform system.** Use the `build` command to deploy:

```bash
# Deploy all platforms (web, iOS, Mac sync)
./build all

# Deploy Mac sync only (this repo)
./build mac

# Preview without deploying
./build all --dry-run
```

## Repository Structure

This is `/Users/jim/GitHub/bob-mac-sync/` - the **native Mac synchronization service** for:
- Syncing reminders with Firestore
- Calendar block notifications
- Background sync daemon

**Related Repos:**
- Main repo: `/Users/jim/GitHub/bob/` (Web UI, Cloud Functions, orchestrator)
- iOS app: `/Users/jim/GitHub/bob-ios/` (Mac Catalyst iOS app)

## Build Targets

When running `./build mac`:
1. ✅ Builds project (Cargo for Rust OR Swift for native)
2. ✅ Code signs the binary with local certificate
3. ✅ Removes old builds to avoid signing conflicts (7-day workaround)
4. ✅ Installs to `/Applications/BOB-SyncService`
5. ✅ Records build details in manifest

## What Agents Should Know

- **Master orchestrator:** `/Users/jim/GitHub/bob/orchestrate-build.sh`
- **Simple command:** `./build` (in any repo)
- **Build logs:** `/Users/jim/GitHub/bob/build-logs/manifest.json`
- **Wrapper script:** `./ORCHESTRATE_BUILD.sh` (points to master)
- **Binary location:** `/Applications/BOB-SyncService`

## Build Workflow for Agents

1. **Make code changes** in this repo
2. **Commit and push**
3. **Run build from any repo:**
   ```bash
   cd /Users/jim/GitHub/bob-mac-sync
   ./build all  # or ./build mac
   ```
4. **Check results:**
   ```bash
   cat /Users/jim/GitHub/bob/build-logs/manifest.json | jq '.commits.mac'
   ```
5. **Service installed at:** `/Applications/BOB-SyncService`

## Key Files

| Path | Purpose |
|------|---------|
| `src/main.rs` OR `main.swift` | Entry point |
| `Cargo.toml` OR `Package.swift` | Dependencies |
| `build-logs/` | Build artifacts (created during build) |

## 7-Day Code Signing Workaround

This repo automatically handles the macOS 7-day certificate signing cycle:
- Each build gets a fresh signature
- Old builds are removed to prevent conflicts
- No manual certificate management needed
- Solves the "certificate expired" issue from multiple builds

## Troubleshooting

- **Build not found:** Ensure `/Users/jim/GitHub/bob/orchestrate-build.sh` exists
- **Code signing fails:** `security unlock-keychain` then retry
- **Cargo/Swift not found:** Install Rust or Xcode command line tools
- **Check versions:** `jq '.versions' /Users/jim/GitHub/bob/build-logs/manifest.json`

## Integration Points

- **Firestore:** Reads tasks/stories marked for syncing
- **Reminders API:** Posts notifications to system reminders
- **Calendar:** Syncs calendar blocks to system calendar
- **Background execution:** Runs as daemon via LaunchAgent

---

**See Also:**
- `/Users/jim/GitHub/bob/BUILD_ORCHESTRATION_GUIDE.md` - Full documentation
- `/Users/jim/GitHub/bob/build --help` - Quick reference
- Master repo: `/Users/jim/GitHub/bob/AGENTS.md`
