#!/bin/bash
# BOB Mac Sync - Build Orchestrator Wrapper
# This script provides a pointer to the master build orchestrator
# Usage: ./ORCHESTRATE_BUILD.sh [OPTIONS]
# All options are passed through to the master orchestrator

MASTER_BUILD_SCRIPT="/Users/jim/GitHub/bob/orchestrate-build.sh"

if [ ! -f "$MASTER_BUILD_SCRIPT" ]; then
    echo "Error: Master build script not found at $MASTER_BUILD_SCRIPT"
    exit 1
fi

# Execute master build script with all arguments
"$MASTER_BUILD_SCRIPT" "$@"
exit $?
