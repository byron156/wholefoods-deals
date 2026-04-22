import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from playwright.sync_api import sync_playwright

from discover_all_deals import (
    ALL_DEALS_URL,
    ProgressBar,
    fast_scroll_to_trigger_next_batch,
    parse_card_recommendation,
    set_store_via_store_modal_url,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe Whole Foods live getGridAsins traffic.")
    parser.add_argument("--store-id", default="10160")
    parser.add_argument("--store-name", default="Columbus Circle")
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--output", default="logs/wfm_network_probe.json")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = {"id": args.store_id, "name": args.store_name}
    captured_batches = []
    asins = set()
    product_api_hits = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def handle_response(response) -> None:
            url = response.url
            if "getGridAsins" in url:
                try:
                    body = json.loads(response.request.post_data or "{}")
                except Exception:
                    return

                batch_asins = []
                for raw in body.get("cardRecommendations", []):
                    try:
                        rec = parse_card_recommendation(raw)
                    except Exception:
                        continue
                    if rec["id"]:
                        batch_asins.append(rec["id"])
                        asins.add(rec["id"])

                captured_batches.append(
                    {
                        "url": url,
                        "batch_size": len(batch_asins),
                        "asins": batch_asins,
                    }
                )
                return

            if "/api/wwos/products" in url:
                product_api_hits.append({"url": url, "status": response.status})

        page.on("response", handle_response)

        set_store_via_store_modal_url(page, ProgressBar(), store=store)
        page.goto(ALL_DEALS_URL, wait_until="domcontentloaded")

        rounds = []
        for round_index in range(1, max(1, args.rounds) + 1):
            before = len(asins)
            did_scroll = fast_scroll_to_trigger_next_batch(page)
            page.wait_for_timeout(900)
            after = len(asins)
            rounds.append(
                {
                    "round": round_index,
                    "did_scroll": bool(did_scroll),
                    "new_asins": max(0, after - before),
                    "total_asins": after,
                }
            )
            if not did_scroll:
                break

        browser.close()

    payload = {
        "store": store,
        "all_deals_url": ALL_DEALS_URL,
        "rounds_requested": args.rounds,
        "total_unique_asins": len(asins),
        "sample_asins": sorted(asins)[:50],
        "batches": captured_batches,
        "rounds": rounds,
        "product_api_hits": product_api_hits,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {output_path}")
    print(f"Store: {args.store_name} ({args.store_id})")
    print(f"Unique ASINs captured live: {len(asins)}")
    print(f"getGridAsins batches: {len(captured_batches)}")
    if captured_batches:
        print("Batch sizes:", [batch["batch_size"] for batch in captured_batches[:12]])
    print(f"/api/wwos/products hits seen: {len(product_api_hits)}")
    if asins:
        print("Sample ASINs:", ", ".join(sorted(asins)[:12]))


if __name__ == "__main__":
    main()
