#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from fixed_taxonomy import build_fixed_taxonomy
from taxonomy_ai import normalize_model_result


def load_json(path, default):
    path = Path(path)
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def save_json(path, value):
    with Path(path).open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2, ensure_ascii=False)
        file.write("\n")


def label_key(label):
    for field in ("asin", "url", "raw_name", "name"):
        value = label.get(field)
        if value:
            return f"{field}:{str(value).strip().casefold()}"
    return None


def reviewed_items_from_queue(queue_payload, taxonomy):
    for item in queue_payload.get("items") or []:
        category = item.get("reviewed_category")
        subcategory = item.get("reviewed_subcategory")
        if not category or not subcategory:
            continue

        normalized = normalize_model_result(
            {
                "category": category,
                "subcategory": subcategory,
                "confidence": 1.0,
                "reasoning": item.get("review_notes") or "Manual taxonomy review.",
            },
            taxonomy,
        )
        if not normalized:
            product_name = (item.get("product") or {}).get("name") or "(unknown product)"
            print(f"Skipping invalid reviewed label for {product_name}: {category} / {subcategory}")
            continue

        product = item.get("product") or {}
        yield {
            "asin": product.get("asin"),
            "url": product.get("url"),
            "name": product.get("name"),
            "raw_name": product.get("raw_name"),
            "brand": product.get("brand"),
            "retailer": product.get("retailer"),
            "category": normalized["category"],
            "subcategory": normalized["subcategory"],
            "notes": item.get("review_notes") or "",
            "source": "manual-review",
        }


def parse_args():
    parser = argparse.ArgumentParser(description="Export filled taxonomy review rows into the gold label set.")
    parser.add_argument("--review-queue", default="taxonomy_review_queue.json")
    parser.add_argument("--gold-labels", default="taxonomy_gold_labels.json")
    return parser.parse_args()


def main():
    args = parse_args()
    taxonomy = build_fixed_taxonomy()
    queue_payload = load_json(args.review_queue, {"items": []})
    existing_payload = load_json(args.gold_labels, {"labels": []})
    existing_labels = existing_payload.get("labels") if isinstance(existing_payload, dict) else existing_payload
    labels_by_key = {}

    for label in existing_labels or []:
        key = label_key(label)
        if key:
            labels_by_key[key] = label

    exported_count = 0
    for label in reviewed_items_from_queue(queue_payload, taxonomy):
        key = label_key(label)
        if not key:
            continue
        labels_by_key[key] = label
        exported_count += 1

    labels = sorted(labels_by_key.values(), key=lambda row: (row.get("retailer") or "", row.get("name") or ""))
    output = {
        "instructions": (
            "Manual taxonomy labels. These override exact products and are weighted into "
            "the local sklearn taxonomy classifier during refresh."
        ),
        "label_count": len(labels),
        "labels": labels,
    }
    save_json(args.gold_labels, output)
    print(f"Wrote {args.gold_labels} with {len(labels)} labels ({exported_count} reviewed rows exported).")


if __name__ == "__main__":
    main()
