#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


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


def normalize_key(value):
    return " ".join(str(value or "").lower().split())


def identity_values(product):
    values = set()
    for field in ("asin", "url", "raw_name", "name", "id"):
        value = product.get(field)
        if value:
            values.add(normalize_key(value))
    for value in product.get("asins") or []:
        if value:
            values.add(normalize_key(value))
    return values


def index_by_identity(rows):
    index = {}
    for row in rows or []:
        for value in identity_values(row):
            index[value] = row
    return index


def find_match(row, index):
    for value in identity_values(row):
        if value in index:
            return index[value]
    return None


def clip_summary(row):
    locked = row.get("clip_locked_pair") or {}
    if not locked.get("category") or not locked.get("subcategory"):
        return None
    return {
        "category": locked.get("category"),
        "subcategory": locked.get("subcategory"),
        "subcategory_category": locked.get("category"),
        "score": locked.get("score") or 0,
        "subcategory_score": locked.get("score") or 0,
        "source": locked.get("source") or "locked",
    }


def compact_product(product, gold_label):
    return {
        "asin": product.get("asin") or gold_label.get("asin"),
        "name": product.get("name") or gold_label.get("name"),
        "raw_name": product.get("raw_name") or gold_label.get("raw_name"),
        "brand": product.get("brand") or gold_label.get("brand"),
        "retailer": product.get("retailer") or gold_label.get("retailer"),
        "sources": product.get("sources") or [],
        "image": product.get("image"),
        "url": product.get("url") or gold_label.get("url"),
        "current_category": gold_label.get("category"),
        "current_subcategory": gold_label.get("subcategory"),
        "confidence": 1.0,
        "reasoning": gold_label.get("notes") or "Existing manual gold label.",
    }


def build_queue(products, gold_labels, clip_report, *, min_clip_score, max_items):
    product_index = index_by_identity(products)
    clip_rows = clip_report.get("results") if isinstance(clip_report, dict) else []
    clip_index = {}
    for row in clip_rows or []:
        product = row.get("product") or {}
        clip = clip_summary(row)
        if not clip:
            continue
        for value in identity_values(product):
            clip_index[value] = clip

    items = []
    for label in gold_labels:
        product = find_match(label, product_index) or label
        clip = find_match(label, clip_index)
        if not clip:
            continue
        score = float(clip.get("score") or 0)
        gold_pair = (normalize_key(label.get("category")), normalize_key(label.get("subcategory")))
        clip_pair = (normalize_key(clip.get("category")), normalize_key(clip.get("subcategory")))
        if score < min_clip_score or gold_pair == clip_pair:
            continue
        items.append(
            {
                "priority": 1 if normalize_key(label.get("category")) != normalize_key(clip.get("category")) else 2,
                "review_reason": (
                    f"Gold label disagrees with high-confidence CLIP ({score:.3f}). "
                    "This may be a real CLIP miss, but it is worth sanity-checking the manual label."
                ),
                "product": compact_product(product, label),
                "clip": clip,
                "reviewed_category": None,
                "reviewed_subcategory": None,
                "review_notes": "",
            }
        )

    items.sort(
        key=lambda item: (
            item["priority"],
            -float((item.get("clip") or {}).get("score") or 0),
            item["product"].get("name") or "",
        )
    )
    return items[:max_items] if max_items else items


def parse_args():
    parser = argparse.ArgumentParser(description="Build a short queue of possible gold-label mistakes.")
    parser.add_argument("--products", default="combined_products.json")
    parser.add_argument("--gold-labels", default="taxonomy_gold_labels.json")
    parser.add_argument("--vision-audit", default="vision_category_audit.full.json")
    parser.add_argument("--output", default="taxonomy_gold_sanity_queue.json")
    parser.add_argument("--min-clip-score", type=float, default=0.80)
    parser.add_argument("--max-items", type=int, default=25)
    return parser.parse_args()


def main():
    args = parse_args()
    products = load_json(args.products, [])
    gold_payload = load_json(args.gold_labels, {"labels": []})
    gold_labels = gold_payload.get("labels") if isinstance(gold_payload, dict) else gold_payload
    clip_report = load_json(args.vision_audit, {"results": []})
    queue = build_queue(
        products,
        gold_labels or [],
        clip_report,
        min_clip_score=args.min_clip_score,
        max_items=args.max_items,
    )
    output = {
        "instructions": (
            "Sanity-check these existing gold labels. Use review_taxonomy_gold_labels.py "
            "with --include-labeled so you can confirm or overwrite them."
        ),
        "min_clip_score": args.min_clip_score,
        "queue_count": len(queue),
        "items": queue,
    }
    save_json(args.output, output)
    print(f"Wrote {args.output} with {len(queue)} possible gold-label conflicts.")


if __name__ == "__main__":
    main()
