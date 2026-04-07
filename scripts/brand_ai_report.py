#!/usr/bin/env python3

import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from brand_ai import build_brand_family_map
from app import (
    BRAND_CONNECTORS,
    BRAND_DESCRIPTOR_STARTERS,
    BRAND_FAMILY_ALIASES,
    GENERIC_BRAND_WORDS,
)

COMBINED_PRODUCTS_FILE = BASE_DIR / "combined_products.json"
OUTPUT_FILE = BASE_DIR / "brand_ai_report.json"


def main():
    products = json.loads(COMBINED_PRODUCTS_FILE.read_text(encoding="utf-8"))
    family_map, cluster_report = build_brand_family_map(
        products,
        alias_map=BRAND_FAMILY_ALIASES,
        connectors=BRAND_CONNECTORS,
        generic_words=GENERIC_BRAND_WORDS,
        descriptor_starters=BRAND_DESCRIPTOR_STARTERS,
    )

    report = {
        "brand_count": len({product.get("brand") for product in products if product.get("brand")}),
        "canonicalized_variants": len(family_map),
        "family_map": dict(sorted(family_map.items())),
        "clusters": cluster_report,
    }
    OUTPUT_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Wrote {OUTPUT_FILE}")
    print(f"Unique brands: {report['brand_count']}")
    print(f"Canonicalized variants: {report['canonicalized_variants']}")
    print("Top learned families:")
    for cluster in cluster_report[:10]:
        members = ", ".join(cluster["members"])
        print(f"- {cluster['canonical']} <= {members}")


if __name__ == "__main__":
    main()
