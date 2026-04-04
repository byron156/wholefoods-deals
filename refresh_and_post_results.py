import json
import os

from app import (
    BASE_DIR,
    build_combined_products,
    fetch_products,
    load_all_deals,
    load_saved_flyer_products,
    load_search_deals,
)
from discover_all_deals import discover_all_deals
from discover_search_deals import discover_search_deals


DISCOVERED_RECOMMENDATIONS_FILE = os.path.join(BASE_DIR, "discovered_recommendations.json")
DISCOVERED_PRODUCTS_FILE = os.path.join(BASE_DIR, "discovered_products.json")
CAPTURED_BATCHES_FILE = os.path.join(BASE_DIR, "captured_batches.json")
SEARCH_DEALS_PRODUCTS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")
SEARCH_DEALS_REPORT_FILE = os.path.join(BASE_DIR, "search_deals_report.json")
FLYER_PRODUCTS_FILE = os.path.join(BASE_DIR, "flyer_products.json")
FLYER_REPORT_FILE = os.path.join(BASE_DIR, "flyer_report.json")
COMBINED_PRODUCTS_FILE = os.path.join(BASE_DIR, "combined_products.json")
COMBINED_REPORT_FILE = os.path.join(BASE_DIR, "combined_report.json")


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main():
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

    print("Building combined products...")
    normalized_flyer_products = load_saved_flyer_products()
    normalized_all_deals_products = load_all_deals()
    normalized_search_deals_products = load_search_deals()

    combined_products = build_combined_products(
        normalized_flyer_products,
        normalized_all_deals_products,
        normalized_search_deals_products,
    )
    write_json(COMBINED_PRODUCTS_FILE, combined_products)
    write_json(
        COMBINED_REPORT_FILE,
        {
            "flyer_count": len(normalized_flyer_products),
            "all_deals_count": len(normalized_all_deals_products),
            "search_deals_count": len(normalized_search_deals_products),
            "combined_unique_count": len(combined_products),
        },
    )

    print("\nRefresh complete.")
    print(f"Flyer products: {len(normalized_flyer_products)}")
    print(f"All deals products: {len(normalized_all_deals_products)}")
    print(f"Search deals products: {len(normalized_search_deals_products)}")
    print(f"Combined unique products: {len(combined_products)}")
    print("\nPages:")
    print("  /")
    print("  /flyer")
    print("  /all-deals")
    print("  /search-deals")


if __name__ == "__main__":
    main()
