#!/bin/bash
# Runs the full YouTube trending pipeline from the repository folder (macOS/Linux).
# Requires: Java 17, a running MySQL server, and YOUTUBE_API_KEY set in your environment.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/pipeline.log"

# Optional: Homebrew Java on macOS (Apple Silicon / Intel)
for J in /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17; do
  if [ -d "$J" ]; then
    export JAVA_HOME="$J"
    export PATH="$J/bin:$PATH"
    break
  fi
done

if [ -z "$YOUTUBE_API_KEY" ]; then
  echo "YOUTUBE_API_KEY is not set. See README.md." >&2
  exit 1
fi

echo "Started at: $(date)" > "$LOG_FILE"

echo "Checking MySQL..."
if command -v brew >/dev/null 2>&1; then
  brew services start mysql 2>/dev/null || true
  sleep 2
fi

echo "Running main pipeline..."
python "$SCRIPT_DIR/youtube_pipeline.py" >> "$LOG_FILE" 2>&1

echo "Pipeline complete! Log: $LOG_FILE"
echo "Finished at: $(date)" >> "$LOG_FILE"
