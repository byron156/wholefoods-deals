import argparse
import json
import os
import re
import sys
from pathlib import Path

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
        print("  /Users/jonathancampbell/Code/wholefoods_deals/.venv/bin/python3 scripts/test_wfm_search_probe.py ...")
    raise

from discover_search_deals import (
    SEARCH_DEALS_URL,
    dismiss_popups,
    launch_browser,
    parse_next_data_products,
    set_store_from_search_page,
)


INTERESTING_TOKENS = [
    "getGridAsins",
    "/api/wwos/products",
    "/api/wwos/",
    "/api/stores/",
    "/stores/search",
    "/grocery/search",
    "search?",
    "relevanceblender",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe Whole Foods search page state and network.")
    parser.add_argument("--store-id", default="10328")
    parser.add_argument("--store-name", default="Upper West Side")
    parser.add_argument("--wait-seconds", type=int, default=20)
    parser.add_argument("--browser", choices=["chrome", "chromium"], default="chrome")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--url", default=SEARCH_DEALS_URL)
    parser.add_argument("--output", default="logs/wfm_search_probe.json")
    parser.add_argument("--max-body-chars", type=int, default=2500)
    return parser


def is_interesting_url(url: str) -> bool:
    return any(token in (url or "") for token in INTERESTING_TOKENS)


def truncate(value: str, max_chars: int) -> str:
    value = value or ""
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "...<truncated>"


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["WHOLEFOODS_SEARCH_BROWSER"] = args.browser
    os.environ["WHOLEFOODS_SEARCH_HEADLESS"] = "true" if args.headless else "false"
    os.environ["WHOLEFOODS_SEARCH_STORE_FLOW"] = "page"

    store = {"id": args.store_id, "name": args.store_name}
    requests = []
    responses = []
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

            entry = {
                "method": req.method,
                "url": url,
                "resource_type": req.resource_type,
                "headers": {
                    key: value
                    for key, value in req.headers.items()
                    if key.lower() in {
                        "accept",
                        "content-type",
                        "origin",
                        "referer",
                        "x-requested-with",
                    }
                },
            }

            try:
                post_data = req.post_data
            except Exception:
                post_data = None

            if post_data:
                entry["post_data_preview"] = truncate(post_data, args.max_body_chars)

            requests.append(entry)

        def handle_response(resp) -> None:
            url = resp.url
            if not is_interesting_url(url):
                return

            entry = {
                "status": resp.status,
                "url": url,
                "content_type": resp.headers.get("content-type"),
                "headers": {
                    key: value
                    for key, value in resp.headers.items()
                    if key.lower() in {
                        "content-type",
                        "cache-control",
                        "location",
                        "server",
                        "x-cache",
                    }
                },
            }

            content_type = (resp.headers.get("content-type") or "").lower()
            if "json" in content_type or any(token in url for token in ["grocery/search", "/api/wwos/", "/stores/search"]):
                try:
                    entry["text_preview"] = truncate(resp.text(), args.max_body_chars)
                except Exception as exc:
                    entry["text_preview_error"] = repr(exc)

            responses.append(entry)

        page.on("request", handle_request)
        page.on("response", handle_response)

        try:
            set_store_from_search_page(page, store)
            page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(max(1, args.wait_seconds) * 1000)
            dismiss_popups(page)
        except Exception as exc:
            setup_error = repr(exc)

        html = page.content()
        body_text = ""
        try:
            body_text = page.locator("body").inner_text(timeout=2500)
        except Exception:
            pass

        next_products = parse_next_data_products(html)
        page_type = None
        next_data_found = False
        page_props_keys = []
        search_results_keys = []

        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
            html,
            flags=re.S,
        )
        if match:
            next_data_found = True
            try:
                data = json.loads(match.group(1))
                page_props = data.get("props", {}).get("pageProps", {})
                page_type = page_props.get("pageType")
                page_props_keys = sorted(page_props.keys())
                search_results_keys = sorted((page_props.get("searchResults") or {}).keys())
            except Exception:
                page_type = "parse_error"

        payload = {
            "store": store,
            "search_url": args.url,
            "wait_seconds": args.wait_seconds,
            "page_url": page.url,
            "setup_error": setup_error,
            "next_data_found": next_data_found,
            "next_data_page_type": page_type,
            "next_data_product_count": len(next_products),
            "page_props_keys": page_props_keys,
            "search_results_keys": search_results_keys,
            "request_count": len(requests),
            "response_count": len(responses),
            "requests": requests,
            "responses": responses,
            "body_sample": body_text[:4000],
        }

        html_path = output_path.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
        payload["html_path"] = str(html_path)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        browser.close()

    print(f"Wrote {output_path}")
    print(f"Store: {args.store_name} ({args.store_id})")
    if setup_error:
        print(f"Setup error: {setup_error}")
    print(f"Page type: {payload['next_data_page_type']}")
    print(f"Next-data product count: {payload['next_data_product_count']}")
    print(f"Requests captured: {len(requests)}")
    print(f"Responses captured: {len(responses)}")


if __name__ == "__main__":
    main()
