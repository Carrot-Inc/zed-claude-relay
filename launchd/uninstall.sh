#!/bin/sh
# Stop the relay's LaunchAgent and remove it.
set -eu
label=com.lermex.zed-claude-relay
plist="$HOME/Library/LaunchAgents/$label.plist"
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
rm -f "$plist"
echo "removed $label"
