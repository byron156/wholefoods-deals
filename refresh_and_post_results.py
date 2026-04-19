import json
import os
import argparse
import subprocess
import sys
import traceback

from app import (
    BASE_DIR,
    build_combined_products,
    fetch_products,
    load_all_deals,
    load_hmart_deals,
    load_saved_flyer_products,
    load_search_deals,
    load_target_deals,
)
from discover_all_deals import discover_all_deals
from discover_hmart_deals import discover_hmart_deals
from discover_search_deals import discover_search_deals
from discover_target_deals import discover_target_deals


DISCOVERED_RECOMMENDATIONS_FILE = os.path.join(BASE_DIR, "discovered_recommendations.json")
DISCOVERED_PRODUCTS_FILE = os.path.join(BASE_DIR, "discovered_products.json")
CAPTURED_BATCHES_FILE = os.path.join(BASE_DIR, "captured_batches.json")
SEARCH_DEALS_PRODUCTS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")
SEARCH_DEALS_REPORT_FILE = os.path.join(BASE_DIR, "search_deals_report.json")
FLYER_PRODUCTS_FILE = os.path.join(BASE_DIR, "flyer_products.json")
FLYER_REPORT_FILE = os.path.join(BASE_DIR, "flyer_report.json")
COMBINED_PRODUCTS_FILE = os.path.join(BASE_DIR, "combined_products.json")
COMBINED_REPORT_FILE = os.path.join(BASE_DIR, "combined_report.json")
CLIP_AUDIT_CANDIDATES_FILE = os.path.join(BASE_DIR, ".cache", "clip_audit_products.json")
CLIP_AUDIT_FILE = os.path.join(BASE_DIR, "vision_category_audit.full.json")
TARGET_DEALS_PRODUCTS_FILE = os.path.join(BASE_DIR, "target_deals_products.json")
TARGET_DEALS_REPORT_FILE = os.path.join(BASE_DIR, "target_deals_report.json")
HMART_DEALS_PRODUCTS_FILE = os.path.join(BASE_DIR, "hmart_deals_products.json")
HMART_DEALS_REPORT_FILE = os.path.join(BASE_DIR, "hmart_deals_report.json")


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def refresh_missing_clip_audit(products):
    os.makedirs(os.path.dirname(CLIP_AUDIT_CANDIDATES_FILE), exist_ok=True)
    write_json(CLIP_AUDIT_CANDIDATES_FILE, products)
    print("Refreshing missing CLIP audit labels before taxonomy classification...")
    subprocess.run(
        [
            sys.executable,
            "-u",
            "vision_category_audit.py",
            "--products",
            CLIP_AUDIT_CANDIDATES_FILE,
            "--output",
            CLIP_AUDIT_FILE,
            "--refresh-missing",
            "--limit",
            "0",
        ],
        cwd=BASE_DIR,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rediscover-taxonomy",
        action="store_true",
        help="Run a full taxonomy re-discovery before classification.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=0,
        help="Only classify a stable sample of merged products for faster testing.",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Reuse existing scraped JSON files instead of scraping again.",
    )
    parser.add_argument(
        "--skip-clip-audit",
        action="store_true",
        help="Do not refresh missing CLIP audit rows before taxonomy classification.",
    )
    args = parser.parse_args()

    if args.skip_refresh:
        print("Skipping scrape refresh and reusing existing JSON files...")
    else:
        print("Refreshing search deals...")
        search_result = discover_search_deals()
        write_json(SEARCH_DEALS_PRODUCTS_FILE, search_result["products"])
        write_json(
            SEARCH_DEALS_REPORT_FILE,
            {
                "search_url": search_result["search_url"],
                "product_count": search_result["product_count"],
                "network_batch_count": search_result["network_batch_count"],
                "sort_runs": search_result.get("sort_runs", []),
            },
        )

        print("Refreshing all deals...")
        all_deals_result = discover_all_deals()
        write_json(DISCOVERED_RECOMMENDATIONS_FILE, all_deals_result["recommendations"])
        write_json(DISCOVERED_PRODUCTS_FILE, all_deals_result["products"])
        write_json(CAPTURED_BATCHES_FILE, all_deals_result["captured_batches"])

        print("Refreshing flyer deals...")
        flyer_products = fetch_products()
        write_json(FLYER_PRODUCTS_FILE, flyer_products)
        write_json(
            FLYER_REPORT_FILE,
            {
                "product_count": len(flyer_products),
            },
        )

        print("Refreshing Target deals...")
        try:
            target_result = discover_target_deals()
        except Exception as exc:
            print(f"Target refresh failed; reusing previous Target deals so the full refresh can continue: {exc}")
            traceback.print_exc()
            previous_products = load_json(TARGET_DEALS_PRODUCTS_FILE, [])
            previous_report = load_json(TARGET_DEALS_REPORT_FILE, {})
            target_result = {
                "source_url": previous_report.get("source_url", "https://www.target.com/c/grocery-deals/-/N-k4uyq"),
                "result_count_text": previous_report.get("result_count_text", "previous Target scrape reused"),
                "product_count": len(previous_products),
                "load_more_clicks": previous_report.get("load_more_clicks", 0),
                "products": previous_products,
                "reused_previous": True,
                "error": str(exc),
            }
        write_json(TARGET_DEALS_PRODUCTS_FILE, target_result["products"])
        write_json(
            TARGET_DEALS_REPORT_FILE,
            {
                "source_url": target_result["source_url"],
                "result_count_text": target_result["result_count_text"],
                "product_count": target_result["product_count"],
                "load_more_clicks": target_result["load_more_clicks"],
                "reused_previous": target_result.get("reused_previous", False),
                "error": target_result.get("error"),
            },
        )

        print("Refreshing H Mart deals...")
        hmart_result = discover_hmart_deals()
        write_json(HMART_DEALS_PRODUCTS_FILE, hmart_result["products"])
        write_json(
            HMART_DEALS_REPORT_FILE,
            {
                "source_urls": hmart_result["source_urls"],
                "product_count": hmart_result["product_count"],
                "runs": hmart_result["runs"],
            },
        )

    print("Building combined products...")
    normalized_flyer_products = load_saved_flyer_products()
    normalized_all_deals_products = load_all_deals()
    normalized_search_deals_products = load_search_deals()
    normalized_target_deals_products = load_target_deals()
    normalized_hmart_deals_products = load_hmart_deals()

    clip_audit_enabled = (
        not args.skip_clip_audit
        and os.getenv("WHOLEFOODS_REFRESH_CLIP_AUDIT", "1").strip().lower()
        not in {"0", "false", "no"}
    )
    if clip_audit_enabled:
        preclassification_products = build_combined_products(
            normalized_flyer_products,
            normalized_all_deals_products,
            normalized_search_deals_products,
            normalized_target_deals_products,
            normalized_hmart_deals_products,
            classify=False,
        )
        refresh_missing_clip_audit(preclassification_products)
    else:
        print("Skipping missing CLIP audit refresh before taxonomy classification.")

    combined_products = build_combined_products(
        normalized_flyer_products,
        normalized_all_deals_products,
        normalized_search_deals_products,
        normalized_target_deals_products,
        normalized_hmart_deals_products,
        force_taxonomy_rediscovery=args.rediscover_taxonomy,
        taxonomy_sample_size=args.sample_size,
    )
    write_json(COMBINED_PRODUCTS_FILE, combined_products)
    write_json(
        COMBINED_REPORT_FILE,
        {
            "flyer_count": len(normalized_flyer_products),
            "all_deals_count": len(normalized_all_deals_products),
            "search_deals_count": len(normalized_search_deals_products),
            "target_deals_count": len(normalized_target_deals_products),
            "hmart_deals_count": len(normalized_hmart_deals_products),
            "combined_unique_count": len(combined_products),
        },
    )

    print("\nRefresh complete.")
    print(f"Flyer products: {len(normalized_flyer_products)}")
    print(f"All deals products: {len(normalized_all_deals_products)}")
    print(f"Search deals products: {len(normalized_search_deals_products)}")
    print(f"Target deals products: {len(normalized_target_deals_products)}")
    print(f"H Mart deals products: {len(normalized_hmart_deals_products)}")
    print(f"Combined unique products: {len(combined_products)}")
    if args.sample_size:
        print(f"Sample size used for taxonomy/classification: {args.sample_size}")
    print("\nPages:")
    print("  /")
    print("  /flyer")
    print("  /all-deals")
    print("  /search-deals")


if __name__ == "__main__":
    main()
