#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Python caches
find "$ROOT" -type d -name '__pycache__' -prune -exec rm -rf {} +
find "$ROOT" -type f -name '*.pyc' -delete

# Runtime artifacts
rm -rf "$ROOT/logs" 2>/dev/null || true
rm -f "$ROOT"/*.csv 2>/dev/null || true

echo "Cleaned: __pycache__/ *.pyc logs/ *.csv"
