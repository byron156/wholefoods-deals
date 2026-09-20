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
- `dist/meal-plan/index.html`
- `dist/static/*`
- `dist/data/*`

## Weekly Meal Planner

Open `/meal-plan/` or select **Weekly meal plan** in the deal browser. The planner
works in both Flask and the static build, with no external AI service required.
It generates seven days of breakfast, lunch, and dinner, scales ingredients for
1–12 people, and consolidates them into a printable, downloadable grocery list.
Choose a retailer, Whole Foods location, mixed or vegetarian menu, and whether
to use Prime prices. Pantry flags, shopping checkmarks, and preferences are saved
in the browser. Rebuilding the menu resets shopping checkmarks.

Recipe selection favors matching discounted ingredients while varying meals.
Missing deals become explicit regular-price purchases. Prices are recorded offer
prices, not a basket estimate; quantities describe recipe needs, not package counts.
The planner excludes dated expired offers and reports known collection failures,
but cannot verify undated offers or guarantee current availability. Rebuild the
static site after refreshing the catalog to update its embedded deals.

Validate the planner with `node --test tests/meal-planner.test.js`.

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

The meal planner defaults to Prime pricing, with a checkbox to opt out. Existing
planner preferences migrate once to this default; subsequent choices are retained.

The digest includes a meal-plan link and a signed unsubscribe confirmation link.
Set `PUBLIC_SITE_BASE_URL` to the public website URL when the API is hosted on a
separate origin. `PUBLIC_API_BASE_URL` must point to the API for email feedback
and unsubscribe links. Keep `APP_SECRET_KEY` stable across deployments so signed
links remain valid. Newsletter cadence uses successful deliveries, and digests
skip missing, expired dated, non-discounted, hidden, and out-of-store offers.
Run `python -m unittest discover -s tests -p 'test_*.py'` for newsletter checks.
