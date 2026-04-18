#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/jonathancampbell/Code/wholefoods_deals"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python3"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_DIR="$PROJECT_DIR/.daily_refresh.lock"

mkdir -p "$LOG_DIR"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

echo "[$(timestamp)] Starting daily Whole Foods refresh"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[$(timestamp)] Missing Python interpreter at $PYTHON_BIN" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[$(timestamp)] git is not installed or not on PATH" >&2
  exit 1
fi

cd "$PROJECT_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(timestamp)] Another refresh appears to be running; exiting without starting a second one."
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[$(timestamp)] $PROJECT_DIR is not a git repository. Initialize or clone the repo before enabling the launchd job." >&2
  exit 1
fi

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "[$(timestamp)] Git remote 'origin' is not configured." >&2
  exit 1
fi

export WHOLEFOODS_SEARCH_MODE="${WHOLEFOODS_SEARCH_MODE:-full}"
echo "[$(timestamp)] Whole Foods search mode: $WHOLEFOODS_SEARCH_MODE"

"$PYTHON_BIN" -u refresh_and_post_results.py
"$PYTHON_BIN" -u build_static_site.py

git add \
  flyer_products.json \
  flyer_report.json \
  discovered_products.json \
  discovered_recommendations.json \
  captured_batches.json \
  search_deals_products.json \
  search_deals_report.json \
  target_deals_products.json \
  target_deals_report.json \
  hmart_deals_products.json \
  hmart_deals_report.json \
  combined_products.json \
  combined_report.json \
  discovered_taxonomy.json \
  taxonomy_classification_cache.json \
  taxonomy_ai_report.json \
  dist

if git diff --cached --quiet; then
  echo "[$(timestamp)] No content changes detected; nothing to commit."
  exit 0
fi

commit_message="Daily Whole Foods refresh $(date +%Y-%m-%d)"
git commit -m "$commit_message"
git push origin main

echo "[$(timestamp)] Daily Whole Foods refresh completed successfully"
