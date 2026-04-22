import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError as exc:
    if exc.name == "playwright":
        print("Playwright is not installed in the Python interpreter that launched this script.")
        print(f"Current interpreter: {sys.executable}")
        print("Try running with the repo venv directly instead:")
        print("  /Users/jonathancampbell/Code/wholefoods_deals/.venv/bin/python3 scripts/test_wfm_search_scroll_interceptor.py ...")
    raise

from discover_search_deals import (
    SEARCH_DEALS_URL,
    click_load_more,
    current_rendered_product_count,
    dismiss_popups,
    launch_browser,
    merge_products_from_current_page,
    parse_next_data_products,
    scroll_page_down,
    set_store_from_search_page,
)

ASIN_PATTERN = re.compile(r"\bB[0-9A-Z]{9}\b")
INTERESTING_URL_TOKENS = [
    "/api/wwos/products",
    "/api/wwos/",
    "/grocery/search",
    "/stores/search",
    "getGridAsins",
    "relevanceblender",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Auto-scroll Whole Foods search and intercept ASIN/product-bearing requests.")
    parser.add_argument("--store-id", default="10160")
    parser.add_argument("--store-name", default="Columbus Circle")
    parser.add_argument("--browser", choices=["chrome", "chromium"], default="chrome")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--url", default=SEARCH_DEALS_URL)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--wait-ms", type=int, default=1800)
    parser.add_argument("--max-body-chars", type=int, default=3000)
    parser.add_argument("--output", default="logs/wfm_search_scroll_interceptor.json")
    return parser


def truncate(value: str, max_chars: int) -> str:
    value = value or ""
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "...<truncated>"


def is_interesting_url(url: str) -> bool:
    return any(token in (url or "") for token in INTERESTING_URL_TOKENS)


def extract_asins_from_text(value: str) -> list[str]:
    return sorted(set(ASIN_PATTERN.findall(value or "")))


def endpoint_key(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    os.environ["WHOLEFOODS_SEARCH_BROWSER"] = args.browser
    os.environ["WHOLEFOODS_SEARCH_HEADLESS"] = "true" if args.headless else "false"
    os.environ["WHOLEFOODS_SEARCH_STORE_FLOW"] = "page"

    store = {"id": args.store_id, "name": args.store_name}
    started_at = time.monotonic()

    request_events = []
    response_events = []
    endpoint_stats = Counter()
    network_asins = set()
    dom_asins = set()
    next_data_asins = set()
    setup_error = None

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1100},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = context.new_page()

        def handle_request(req) -> None:
            url = req.url
            if not is_interesting_url(url):
                return

            post_data = None
            try:
                post_data = req.post_data
            except Exception:
                post_data = None

            body_asins = extract_asins_from_text(post_data or "")
            if body_asins:
                network_asins.update(body_asins)

            request_events.append(
                {
                    "ts_s": round(time.monotonic() - started_at, 3),
                    "method": req.method,
                    "url": url,
                    "endpoint": endpoint_key(url),
                    "resource_type": req.resource_type,
                    "body_asin_count": len(body_asins),
                    "body_asins": body_asins[:30],
                    "post_data_preview": truncate(post_data or "", args.max_body_chars) if post_data else None,
                }
            )
            endpoint_stats[f"REQ {endpoint_key(url)}"] += 1

        def handle_response(resp) -> None:
            url = resp.url
            if not is_interesting_url(url):
                return

            text_preview = None
            body_asins = []
            try:
                content_type = (resp.headers.get("content-type") or "").lower()
                if "json" in content_type or "/grocery/search" in url or "/api/wwos/" in url:
                    text_preview = truncate(resp.text(), args.max_body_chars)
                    body_asins = extract_asins_from_text(text_preview)
            except Exception:
                text_preview = None
                body_asins = []

            if body_asins:
                network_asins.update(body_asins)

            response_events.append(
                {
                    "ts_s": round(time.monotonic() - started_at, 3),
                    "status": resp.status,
                    "url": url,
                    "endpoint": endpoint_key(url),
                    "body_asin_count": len(body_asins),
                    "body_asins": body_asins[:30],
                    "text_preview": text_preview,
                }
            )
            endpoint_stats[f"RESP {endpoint_key(url)}"] += 1

        page.on("request", handle_request)
        page.on("response", handle_response)

        rounds = []

        try:
            set_store_from_search_page(page, store)
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(args.wait_ms)
            dismiss_popups(page)

            initial_html = page.content()
            next_data_products = parse_next_data_products(initial_html)
            next_data_asins.update(
                product.get("asin")
                for product in next_data_products
                if product.get("asin")
            )
            merge_products_from_current_page(page, {asin: {"asin": asin} for asin in []})

            bootstrap_products = {}
            merge_products_from_current_page(page, bootstrap_products)
            dom_asins.update(asin for asin in bootstrap_products if asin)

            rounds.append(
                {
                    "round": 0,
                    "label": "initial",
                    "elapsed_s": round(time.monotonic() - started_at, 3),
                    "rendered_count": current_rendered_product_count(page),
                    "next_data_asin_count": len(next_data_asins),
                    "dom_asin_count": len(dom_asins),
                    "network_asin_count": len(network_asins),
                    "did_scroll": False,
                    "clicked_load_more": False,
                }
            )

            products_by_asin = dict(bootstrap_products)

            for round_index in range(1, max(1, args.rounds) + 1):
                did_scroll = scroll_page_down(page)
                page.wait_for_timeout(args.wait_ms)
                dismiss_popups(page)

                clicked_load_more = click_load_more(page)
                if clicked_load_more:
                    page.wait_for_timeout(args.wait_ms)
                    dismiss_popups(page)

                html = page.content()
                round_next_data = parse_next_data_products(html)
                next_data_asins.update(
                    product.get("asin")
                    for product in round_next_data
                    if product.get("asin")
                )
                merge_products_from_current_page(page, products_by_asin)
                dom_asins.update(asin for asin in products_by_asin if asin)

                rounds.append(
                    {
                        "round": round_index,
                        "elapsed_s": round(time.monotonic() - started_at, 3),
                        "rendered_count": current_rendered_product_count(page),
                        "next_data_asin_count": len(next_data_asins),
                        "dom_asin_count": len(dom_asins),
                        "network_asin_count": len(network_asins),
                        "did_scroll": bool(did_scroll),
                        "clicked_load_more": bool(clicked_load_more),
                    }
                )

                if not did_scroll and not clicked_load_more:
                    break
        except Exception as exc:
            setup_error = repr(exc)

        final_html_path = output_path.with_suffix(".html")
        final_html_path.write_text(page.content(), encoding="utf-8")
        browser.close()

    request_with_asins = [event for event in request_events if event["body_asin_count"] > 0]
    response_with_asins = [event for event in response_events if event["body_asin_count"] > 0]

    payload = {
        "store": store,
        "url": args.url,
        "setup_error": setup_error,
        "rounds_requested": args.rounds,
        "wait_ms": args.wait_ms,
        "total_elapsed_s": round(time.monotonic() - started_at, 3),
        "initial_next_data_asin_count": rounds[0]["next_data_asin_count"] if rounds else 0,
        "final_next_data_asin_count": len(next_data_asins),
        "final_dom_asin_count": len(dom_asins),
        "final_network_asin_count": len(network_asins),
        "sample_next_data_asins": sorted(next_data_asins)[:50],
        "sample_dom_asins": sorted(dom_asins)[:50],
        "sample_network_asins": sorted(network_asins)[:50],
        "request_events_with_asins": request_with_asins,
        "response_events_with_asins": response_with_asins,
        "request_events": request_events,
        "response_events": response_events,
        "endpoint_stats": dict(endpoint_stats),
        "rounds": rounds,
        "html_path": str(final_html_path),
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {output_path}")
    print(f"Store: {args.store_name} ({args.store_id})")
    if setup_error:
        print(f"Setup error: {setup_error}")
    print(f"Initial Next.js ASIN count: {payload['initial_next_data_asin_count']}")
    print(f"Final Next.js ASIN count: {payload['final_next_data_asin_count']}")
    print(f"Final DOM ASIN count: {payload['final_dom_asin_count']}")
    print(f"Final network ASIN count: {payload['final_network_asin_count']}")
    print(f"Requests with ASINs in body: {len(request_with_asins)}")
    print(f"Responses with ASINs in body: {len(response_with_asins)}")
    if endpoint_stats:
        print("Top endpoints:")
        for key, count in endpoint_stats.most_common(12):
            print(f"  {count:>3}  {key}")


if __name__ == "__main__":
    main()
