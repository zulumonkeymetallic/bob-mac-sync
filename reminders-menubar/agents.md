# Deployment Rules & Agents

## Overview
This document outlines the deployment process for the `reminders-menubar` application (Bob Mac Sync). It serves as a guide for future agents and developers to ensure consistent builds and deployments.

## Build Requirements
- **Environment**: macOS
- **Tool**: `xcodebuild`
- **Configuration**: Release
- **Scheme**: Reminders MenuBar (or equivalent main scheme)

## Deployment Path
All production builds must be deployed to the following iCloud Drive path:
`/Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync`

## Build Process
1. **Clean**: Always clean the build folder and derived data to ensure a fresh build.
2. **Version**: Ensure `AppConstants.swift` reflects the correct version or formatting logic.
3. **Build Date**: The application is configured to display the build date based on the executable's modification time. Ensure the build process updates the executable.
4. **Artifact Destination**: 
   - Create a subfolder with the current date/time format: `YYYY-MM-DD_HH-mm-ss`
   - Example: `/Users/jim/Library/Mobile Documents/com~apple~CloudDocs/bobmacsync/2026-02-14_18-30-00`
   - **Crucial**: Place ONLY the built `.app` bundle inside this folder. Exclude `.dSYM` files or any other build artifacts.

## Verification
- Verify the "About" section in the running app shows the correct version and build date.
- Ensure the app launches correctly from the deployment location.
