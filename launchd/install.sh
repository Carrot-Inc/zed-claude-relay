#!/bin/sh
# Install (or reinstall) the relay as a macOS LaunchAgent that starts at login and stays up.
set -eu
label=com.lermex.zed-claude-relay
root=$(cd "$(dirname "$0")/.." && pwd)
python=$(command -v python3)
plist="$HOME/Library/LaunchAgents/$label.plist"
logfile="$HOME/Library/Logs/zed-claude-relay.log"
port=${ZED_CLAUDE_RELAY_PORT:-7865}
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"

launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$python</string>
    <string>$root/relay.py</string>
    <string>--port</string>
    <string>$port</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>StandardOutPath</key><string>$logfile</string>
  <key>StandardErrorPath</key><string>$logfile</string>
</dict>
</plist>
EOF
launchctl bootstrap "gui/$(id -u)" "$plist"
launchctl kickstart -k "gui/$(id -u)/$label"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl -s --max-time 2 "http://127.0.0.1:$port/" >/dev/null 2>&1; then
    echo "relay up on http://127.0.0.1:$port (log: $logfile)"
    exit 0
  fi
  sleep 1
done
echo "relay did not answer on port $port; see $logfile" >&2
exit 1
