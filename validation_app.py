import os
import json
from app import fetch_products, load_all_deals, validate_product_fields

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "product_validation_report.json")


def main():
    flyer_products = fetch_products()
    all_deals_products = load_all_deals()

    flyer_problems = validate_product_fields(flyer_products, "flyer")
    all_deals_problems = validate_product_fields(all_deals_products, "all_deals")

    report = {
        "flyer_count": len(flyer_products),
        "all_deals_count": len(all_deals_products),
        "flyer_problem_count": len(flyer_problems),
        "all_deals_problem_count": len(all_deals_problems),
        "flyer_problems": flyer_problems,
        "all_deals_problems": all_deals_problems,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Wrote validation report to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()