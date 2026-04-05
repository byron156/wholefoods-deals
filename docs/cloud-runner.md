# Cloud Runner Plan

This repo now has an iPhone-first app shell on `/`, but the refresh pipeline still runs locally.
When you are ready to move updates off-device, use this target shape:

## Recommended Production Split

- Cloudflare Pages:
  - serves the customer-facing frontend
  - deploys code from GitHub
- Small VPS or VM:
  - runs the Python Playwright scrapers
  - runs the normalization and recommendation refresh jobs
  - writes data into Supabase instead of publishing by Git push
- Supabase:
  - stores products, offers, preferences, saves, feedback events, and recommendation snapshots

## Suggested VPS Setup

1. Ubuntu VM with 2 GB RAM is enough to start.
2. Install:
   - Python 3.11+
   - Node.js
   - Playwright browsers
3. Clone this repo.
4. Create a dedicated `.env` with:
   - Supabase URL
   - Supabase service role key
   - any future store configuration
5. Use `cron` or `systemd timers` to run:
   - flyer scrape
   - all deals scrape
   - search deals scrape
   - normalize/upsert job
   - recommendation refresh job

## Suggested Job Order

1. `discover_search_deals.py`
2. `discover_all_deals.py`
3. flyer fetch via `refresh_and_post_results.py` extraction pieces
4. normalize offers into database rows
5. refresh recommendation snapshots

## Failure Rules

- Failed scrape should not delete last known good offers.
- Each run should write a `scrape_runs` row with diagnostics.
- Recommendation refresh should only run after at least one source succeeded.

## Migration Path

1. Keep current JSON files as backup/debug artifacts.
2. Add a `sync_to_supabase.py` job that converts the normalized data into database upserts.
3. Switch the homepage app shell to fetch live API data.
4. Retire Git-push-driven content publishing once the database-backed app is stable.
