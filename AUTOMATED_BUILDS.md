# BOB Reminders MenuBar - Automated 7-Day Build System

## Setup Complete ✓

Your Mac app now automatically builds and deploys a signed version every 7 days. This handles the macOS certificate signing cycle automatically.

## How It Works

1. **LaunchAgent** (`com.bob.reminders-menubar.build-deploy`) runs every 7 days (604,800 seconds)
2. **Build script** compiles a fresh signed version via `xcodebuild`
3. **Clean cycle** removes builds older than 7 days to prevent signing conflicts
4. **Deploy** copies the new app to `/Applications/BOB-SyncService`
5. **Logs** all activity to `~/.logs/bob-build-deploy.stdout` and `.stderr`

## Files Created

- `scripts/build_and_deploy.sh` - Main build & deployment script
- `~/.logs/` - Build logs directory
- `~/.logs/bob-build-deploy.stdout` - Standard output log
- `~/.logs/bob-build-deploy.stderr` - Error log
- `~/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist` - System scheduler

## Managing Automated Builds

### View Logs
```bash
# View deployment log
tail -f ~/.logs/bob-build-deploy.stdout

# View errors
tail -f ~/.logs/bob-build-deploy.stderr

# Check build history
cd /Users/jim/GitHub/bob-mac-sync && cat .logs/deploy.log
```

### Manually Trigger a Build (Don't Wait 7 Days)
```bash
/Users/jim/GitHub/bob-mac-sync/scripts/build_and_deploy.sh
```

### Check LaunchAgent Status
```bash
# Verify it's loaded
launchctl list | grep "bob.reminders"

# Run immediately (for testing)
launchctl start com.bob.reminders-menubar.build-deploy
```

### Unload/Reload Service
```bash
# Unload (disable automated builds)
launchctl unload /Users/jim/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist

# Reload (re-enable automated builds)
launchctl load /Users/jim/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist

# Force reload (if changes made to plist)
launchctl unload /Users/jim/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist
launchctl load /Users/jim/Library/LaunchAgents/com.bob.reminders-menubar.build-deploy.plist
```

## App Location

The automatically built app is installed to:
- **Path**: `/Applications/BOB-SyncService`
- **Updated**: Every 7 days automatically
- **Latest build also in iCloud**: `/Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync/[timestamp]/`

## Build Details

- **Signing**: Automatic code signing with Apple Development certificate (Team ID: `J877BRYKD6`)
- **Configuration**: Release build with dSYM debug symbols
- **Architecture**: arm64 (Apple Silicon)
- **Scheme**: Reminders MenuBar

## Troubleshooting

### Build Failed?
Check the error logs:
```bash
tail -100 ~/.logs/bob-build-deploy.stderr
cat /Users/jim/GitHub/bob-mac-sync/.logs/deploy.log
```

### Old Builds Not Cleaned?
Manual cleanup (removes builds older than 7 days):
```bash
find /Users/jim/Library/Mobile\ Documents/com~apple~CloudDocs/bobmacsync -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \;
```

### Xcode Tools Missing?
```bash
sudo xcode-select --install
```

## Next Steps

- Monitor the first automated run in 7 days
- Check logs regularly: `tail ~/.logs/bob-build-deploy.stdout`
- Set a calendar reminder to verify builds are happening
