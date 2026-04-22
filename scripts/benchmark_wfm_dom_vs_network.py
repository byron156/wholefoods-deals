import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from playwright.sync_api import sync_playwright

from discover_all_deals import (
    ALL_DEALS_URL,
    ProgressBar,
    fast_scroll_to_trigger_next_batch,
    parse_all_deals_html,
    parse_card_recommendation,
    set_store_via_store_modal_url,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark Whole Foods DOM cards vs live network ASIN capture.")
    parser.add_argument("--store-id", default="10160")
    parser.add_argument("--store-name", default="Columbus Circle")
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--wait-ms", type=int, default=900)
    parser.add_argument("--output", default="logs/wfm_dom_vs_network_benchmark.json")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = {"id": args.store_id, "name": args.store_name}
    network_asins = set()
    batches = []

    started_at = time.monotonic()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def handle_response(response) -> None:
            if "getGridAsins" not in response.url:
                return
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
                    network_asins.add(rec["id"])

            batches.append(
                {
                    "url": response.url,
                    "batch_size": len(batch_asins),
                    "captured_at_s": round(time.monotonic() - started_at, 3),
                }
            )

        page.on("response", handle_response)

        set_store_via_store_modal_url(page, ProgressBar(), store=store)
        page.goto(ALL_DEALS_URL, wait_until="domcontentloaded")

        rounds = []
        for round_index in range(1, max(1, args.rounds) + 1):
            round_start = time.monotonic()
            before_network = len(network_asins)

            did_scroll = fast_scroll_to_trigger_next_batch(page)
            page.wait_for_timeout(args.wait_ms)

            html = page.content()
            dom_products = parse_all_deals_html(html)
            dom_asins = {product.get("asin") for product in dom_products if product.get("asin")}
            after_network = len(network_asins)

            rounds.append(
                {
                    "round": round_index,
                    "did_scroll": bool(did_scroll),
                    "elapsed_s": round(time.monotonic() - started_at, 3),
                    "round_duration_s": round(time.monotonic() - round_start, 3),
                    "dom_product_count": len(dom_products),
                    "dom_asin_count": len(dom_asins),
                    "network_asin_count": after_network,
                    "network_new_asins": max(0, after_network - before_network),
                    "network_lead_vs_dom": after_network - len(dom_asins),
                }
            )

            if not did_scroll:
                break

        final_html = page.content()
        final_dom_products = parse_all_deals_html(final_html)
        final_dom_asins = {product.get("asin") for product in final_dom_products if product.get("asin")}

        browser.close()

    payload = {
        "store": store,
        "rounds_requested": args.rounds,
        "wait_ms": args.wait_ms,
        "total_elapsed_s": round(time.monotonic() - started_at, 3),
        "final_dom_product_count": len(final_dom_products),
        "final_dom_asin_count": len(final_dom_asins),
        "final_network_asin_count": len(network_asins),
        "network_minus_dom": len(network_asins) - len(final_dom_asins),
        "batches": batches,
        "rounds": rounds,
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {output_path}")
    print(f"Store: {args.store_name} ({args.store_id})")
    print(f"Total elapsed: {payload['total_elapsed_s']}s")
    print(f"Final DOM ASIN count: {payload['final_dom_asin_count']}")
    print(f"Final network ASIN count: {payload['final_network_asin_count']}")
    print(f"Network lead vs DOM: {payload['network_minus_dom']}")
    print()
    print("Per round:")
    for row in rounds:
        print(
            f"  round {row['round']:>2}: "
            f"dom={row['dom_asin_count']:<4} "
            f"network={row['network_asin_count']:<4} "
            f"new_network={row['network_new_asins']:<3} "
            f"lead={row['network_lead_vs_dom']:<4} "
            f"elapsed={row['elapsed_s']}s"
        )


if __name__ == "__main__":
    main()
