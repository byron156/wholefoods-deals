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


def product_keys(product):
    keys = []
    for key in ("asin", "url", "name", "raw_name"):
        value = product.get(key)
        if value:
            keys.append(str(value).lower())
    return keys


def candidate_key(product):
    return product.get("asin") or product.get("url") or product.get("raw_name") or product.get("name")


def index_products(products):
    by_key = {}
    for product in products:
        for key in product_keys(product):
            by_key.setdefault(key, product)
    return by_key


def find_product(audit_product, by_key):
    for key in ("id", "name", "raw_name"):
        value = audit_product.get(key)
        if value and str(value).lower() in by_key:
            return by_key[str(value).lower()]
    return None


def build_clip_summary(item):
    locked_pair = item.get("clip_locked_pair") or {}
    clip_category = item.get("clip_category") or {}
    suggested_category = locked_pair.get("category") or clip_category.get("category")
    suggested_subcategory = locked_pair.get("subcategory") or (item.get("clip_subcategory") or {}).get("subcategory")
    if not suggested_category or not suggested_subcategory:
        return None

    return {
        "category": suggested_category,
        "score": locked_pair.get("score") or clip_category.get("score") or 0,
        "source": locked_pair.get("source"),
        "current_rank": (clip_category.get("current_score") or {}).get("rank"),
        "subcategory": suggested_subcategory,
        "subcategory_category": suggested_category,
        "subcategory_score": (locked_pair.get("subcategory_score") or {}).get("score")
        or (item.get("clip_subcategory") or {}).get("score"),
        "top_categories": clip_category.get("top") or [],
        "top_subcategories_in_category": locked_pair.get("top_subcategories_in_category") or [],
    }


def index_clip_results(audit, by_key):
    clips_by_candidate_key = {}
    audit_items_by_candidate_key = {}
    for item in audit.get("results") or []:
        audit_product = item.get("product") or {}
        product = find_product(audit_product, by_key)
        if not product:
            continue
        key = candidate_key(product)
        if not key:
            continue
        clip = build_clip_summary(item)
        if clip:
            clips_by_candidate_key[key] = clip
            audit_items_by_candidate_key[key] = item
    return clips_by_candidate_key, audit_items_by_candidate_key


def compact_product(product):
    return {
        "asin": product.get("asin"),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "brand": product.get("brand"),
        "retailer": product.get("retailer"),
        "sources": product.get("sources") or [],
        "image": product.get("image"),
        "url": product.get("url"),
        "current_category": product.get("category"),
        "current_subcategory": product.get("subcategory"),
        "confidence": product.get("ai_confidence") or product.get("category_confidence") or 0,
        "reasoning": product.get("ai_reasoning") or "",
    }


def add_candidate(candidates, product, reason, priority, clip=None):
    key = candidate_key(product)
    if not key:
        return
    existing = candidates.get(key)
    entry = {
        "priority": priority,
        "review_reason": reason,
        "product": compact_product(product),
        "clip": clip,
        "reviewed_category": None,
        "reviewed_subcategory": None,
        "review_notes": "",
    }
    if not existing or priority < existing["priority"]:
        candidates[key] = entry
    elif existing and clip and not existing.get("clip"):
        existing["clip"] = clip


def existing_review_index(existing_queue):
    reviewed = {}
    for item in (existing_queue or {}).get("items") or []:
        key = candidate_key(item.get("product") or {})
        if not key:
            continue
        if item.get("reviewed_category") and item.get("reviewed_subcategory"):
            reviewed[key] = {
                "reviewed_category": item.get("reviewed_category"),
                "reviewed_subcategory": item.get("reviewed_subcategory"),
                "review_notes": item.get("review_notes") or "",
            }
    return reviewed


def build_queue(products, audit, *, max_items, existing_queue=None):
    candidates = {}
    by_key = index_products(products)
    clips_by_candidate_key, audit_items_by_candidate_key = index_clip_results(audit, by_key)

    for product in products:
        confidence = product.get("ai_confidence") or product.get("category_confidence") or 0
        if confidence <= 0.35:
            reason = "low-confidence classification"
            priority = 30
            if product.get("category") == "Pantry" and product.get("subcategory") == "Meal Kits & Sides":
                reason = "fallback pantry classification"
                priority = 20
            add_candidate(candidates, product, reason, priority, clip=clips_by_candidate_key.get(candidate_key(product)))

    for key, item in audit_items_by_candidate_key.items():
        product = None
        for candidate_product in products:
            if candidate_key(candidate_product) == key:
                product = candidate_product
                break
        if not product:
            continue
        clip = clips_by_candidate_key.get(key) or {}
        clip_score = clip.get("score") or 0
        current_rank = clip.get("current_rank") or 999
        current_confidence = product.get("ai_confidence") or product.get("category_confidence") or 0
        suggested_category = clip.get("category")
        clip_disagrees = product.get("category") != suggested_category
        if not clip_disagrees:
            continue
        if clip_score >= 0.75 and current_rank >= 5 and current_confidence <= 0.75:
            add_candidate(
                candidates,
                product,
                "strong CLIP disagreement on low/medium-confidence item",
                10,
                clip=clip,
            )

    reviewed = existing_review_index(existing_queue)
    for key, values in reviewed.items():
        if key in candidates:
            candidates[key].update(values)

    queue = sorted(
        candidates.values(),
        key=lambda item: (
            item["priority"],
            -(item.get("clip") or {}).get("score", 0),
            item["product"].get("confidence") or 0,
            item["product"].get("name") or "",
        ),
    )
    return queue[:max_items] if max_items else queue


def parse_args():
    parser = argparse.ArgumentParser(description="Build a hand-review queue for taxonomy labels.")
    parser.add_argument("--products", default="combined_products.json")
    parser.add_argument("--vision-audit", default="vision_category_audit.full.json")
    parser.add_argument("--output", default="taxonomy_review_queue.json")
    parser.add_argument("--max-items", type=int, default=300)
    return parser.parse_args()


def main():
    args = parse_args()
    products = load_json(args.products, [])
    audit = load_json(args.vision_audit, {"results": []})
    existing_queue = load_json(args.output, {"items": []})
    queue = build_queue(products, audit, max_items=args.max_items, existing_queue=existing_queue)
    output = {
        "instructions": (
            "Fill reviewed_category and reviewed_subcategory for reviewed items. "
            "Those reviewed labels can become the gold set for evaluating future classifier changes."
        ),
        "product_count": len(products),
        "queue_count": len(queue),
        "items": queue,
    }
    save_json(args.output, output)
    print(f"Wrote {args.output} with {len(queue)} review items.")


if __name__ == "__main__":
    main()
