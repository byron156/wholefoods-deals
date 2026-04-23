# Deployment

## Daily Scrape on macOS

The daily job is driven by:

- wrapper script: [`scripts/daily_refresh.sh`](/Users/jonathancampbell/Code/wholefoods_deals/scripts/daily_refresh.sh)
- launchd plist in repo: [`launchd/com.jonathancampbell.wholefoods-refresh.plist`](/Users/jonathancampbell/Code/wholefoods_deals/launchd/com.jonathancampbell.wholefoods-refresh.plist)
- installed plist: `~/Library/LaunchAgents/com.jonathancampbell.wholefoods-refresh.plist`

The plist is configured for `09:00` local time every day.

Useful commands:

```bash
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.jonathancampbell.wholefoods-refresh.plist
launchctl kickstart -k gui/$UID/com.jonathancampbell.wholefoods-refresh
launchctl print gui/$UID/com.jonathancampbell.wholefoods-refresh
launchctl bootout gui/$UID/com.jonathancampbell.wholefoods-refresh
```

Logs are written to:

- `logs/daily_refresh.stdout.log`
- `logs/daily_refresh.stderr.log`

Important:

- The wrapper expects this folder to be a real Git repository.
- It expects an `origin` remote to exist.
- It pushes to `origin main`.

## Static Site Build

Build the static site with:

```bash
python3 build_static_site.py
```

That generates:

- `dist/index.html`
- `dist/flyer/index.html`
- `dist/all-deals/index.html`
- `dist/combined-products/index.html`
- `dist/static/*`
- `dist/data/*`

## Local AI Taxonomy Runtime

The taxonomy/classification pipeline is now designed to use a local Ollama model during refresh time, not during website requests.

Recommended setup:

```bash
brew install ollama
ollama serve
ollama pull gemma3:4b
```

Notes:

- Daily refreshes classify only new/changed products against the active cached taxonomy.
- A full taxonomy rediscovery is manual:

```bash
python3 refresh_and_post_results.py --rediscover-taxonomy
```

- Ollama must be running for the first taxonomy discovery and for any uncached product classification. The pipeline no longer bootstraps from old catalog labels.

## Cloudflare Pages

Recommended Pages setup:

- Connect the GitHub repository containing this project
- Production branch: `main`
- Build command: none
- Output directory: `dist`

The daily flow is:

1. `refresh_and_post_results.py`
2. `build_static_site.py`
3. `git add` generated JSON + `dist/`
4. `git commit`
5. `git push origin main`
6. Cloudflare Pages deploys automatically from GitHub
