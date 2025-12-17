#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# Copy env template if needed
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from .env.example. Edit it before running."
fi

python -m sniper_bot.main --config configs/sniper.yaml
