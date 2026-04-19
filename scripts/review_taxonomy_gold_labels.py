#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
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


def normalize_key(value):
    return " ".join(str(value or "").lower().split())


def label_key(label):
    for field in ("asin", "url", "raw_name", "name"):
        value = label.get(field)
        if value:
            return f"{field}:{normalize_key(value)}"
    return None


def product_label_key(product):
    return label_key(product)


def index_existing_labels(payload):
    labels = payload.get("labels") if isinstance(payload, dict) else payload
    indexed = {}
    for label in labels or []:
        key = label_key(label)
        if key:
            indexed[key] = label
    return indexed


def taxonomy_maps(taxonomy):
    categories = taxonomy["categories"]
    category_names = [category["name"] for category in categories]
    subcategories = {
        category["name"]: [subcategory["name"] for subcategory in category.get("subcategories") or []]
        for category in categories
    }
    return category_names, subcategories


def clear_screen(enabled=True):
    if enabled:
        os.system("clear")


def display_options(options, columns=1):
    for index, option in enumerate(options, start=1):
        print(f"{index:>2}. {option}")


def choose_option(prompt, options, *, default=None, allow_skip=True):
    normalized = {normalize_key(option): option for option in options}
    while True:
        suffix = ""
        if default:
            suffix = f" [Enter={default}]"
        raw = input(f"{prompt}{suffix}: ").strip()
        if not raw and default:
            return default
        command = raw.lower()
        if allow_skip and command in {"s", "skip"}:
            return None
        if command in {"q", "quit", "exit"}:
            raise KeyboardInterrupt
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(options):
                return options[index - 1]
        raw_key = normalize_key(raw)
        if raw_key in normalized:
            return normalized[raw_key]
        matches = [option for option in options if raw_key and raw_key in normalize_key(option)]
        if len(matches) == 1:
            return matches[0]
        if matches:
            print("Matches:")
            display_options(matches[:12])
        else:
            print("Type a number, exact name, unique search text, s=skip, q=quit.")


def suggested_pair(item):
    product = item.get("product") or {}
    clip = item.get("clip") or {}
    suggestions = []
    if clip.get("category") and clip.get("subcategory") and clip.get("subcategory_category") == clip.get("category"):
        suggestions.append(("clip", clip["category"], clip["subcategory"]))
    if product.get("current_category") and product.get("current_subcategory"):
        suggestions.append(("current", product["current_category"], product["current_subcategory"]))
    return suggestions


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


def build_clip_lookup(path):
    payload = load_json(path, {"results": []})
    rows = payload.get("results") if isinstance(payload, dict) else []
    lookup = {}
    for row in rows or []:
        product = row.get("product") or {}
        clip = row.get("clip_locked_pair") or {}
        if not clip:
            continue
        value = {
            "category": clip.get("category"),
            "subcategory": clip.get("subcategory"),
            "subcategory_category": clip.get("category"),
            "score": clip.get("score"),
            "subcategory_score": clip.get("score"),
            "source": clip.get("source") or "locked",
        }
        for key in identity_values(product):
            lookup[key] = value
    return lookup


def queue_item_from_product(product, clip_lookup):
    clip = None
    for key in identity_values(product):
        if key in clip_lookup:
            clip = clip_lookup[key]
            break
    return {
        "review_reason": product.get("failed_reason") or product.get("classification_status") or "failed classification",
        "product": {
            **product,
            "current_category": product.get("failed_from_category") or product.get("category"),
            "current_subcategory": product.get("failed_from_subcategory") or product.get("subcategory"),
            "confidence": product.get("ai_confidence") or product.get("category_confidence"),
        },
        "clip": clip or {},
    }


def queue_items_from_payload(payload, clip_lookup):
    if isinstance(payload, dict):
        return list(payload.get("items") or [])
    if isinstance(payload, list):
        return [queue_item_from_product(product, clip_lookup) for product in payload if isinstance(product, dict)]
    return []


def print_item(index, total, item, existing_label=None):
    product = item.get("product") or {}
    clip = item.get("clip") or {}
    print(f"Item {index}/{total}")
    print("=" * 80)
    print(product.get("name") or "(unnamed product)")
    if product.get("brand") or product.get("retailer"):
        print(f"Brand/Retailer: {product.get('brand') or '-'} / {product.get('retailer') or '-'}")
    print(f"Reason: {item.get('review_reason')}")
    print(
        "Current: "
        f"{product.get('current_category')} / {product.get('current_subcategory')} "
        f"(conf {product.get('confidence')})"
    )
    if clip:
        print(
            "CLIP:    "
            f"{clip.get('category')} / {clip.get('subcategory')} "
            f"(score {clip.get('score')}, sub {clip.get('subcategory_score')}, {clip.get('source') or 'locked'})"
        )
    if product.get("image"):
        print(f"Image: {product.get('image')}")
    if product.get("url"):
        print(f"URL:   {product.get('url')}")
    if existing_label:
        print(f"Existing gold: {existing_label.get('category')} / {existing_label.get('subcategory')}")
    print("-" * 80)


def valid_label(category, subcategory, taxonomy):
    normalized = normalize_model_result(
        {
            "category": category,
            "subcategory": subcategory,
            "confidence": 1.0,
            "reasoning": "Manual taxonomy review.",
        },
        taxonomy,
    )
    return normalized


def make_label(item, category, subcategory, taxonomy):
    product = item.get("product") or {}
    normalized = valid_label(category, subcategory, taxonomy)
    if not normalized:
        return None
    return {
        "asin": product.get("asin"),
        "url": product.get("url"),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "brand": product.get("brand"),
        "retailer": product.get("retailer"),
        "category": normalized["category"],
        "subcategory": normalized["subcategory"],
        "notes": "",
        "source": "manual-review-cli",
    }


def save_gold_labels(path, labels_by_key):
    labels = sorted(labels_by_key.values(), key=lambda row: (row.get("retailer") or "", row.get("name") or ""))
    save_json(
        path,
        {
            "instructions": (
                "Manual taxonomy labels. These override exact products and are weighted into "
                "the local sklearn taxonomy classifier during refresh."
            ),
            "label_count": len(labels),
            "labels": labels,
        },
    )


def update_review_queue(queue_path, item, category, subcategory):
    payload = load_json(queue_path, {"items": []})
    target_key = product_label_key(item.get("product") or {})
    if not target_key:
        return
    rows = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return
    for row in rows:
        row_product = row.get("product") if isinstance(row.get("product"), dict) else row
        if product_label_key(row_product or {}) == target_key:
            if isinstance(row, dict):
                row["reviewed_category"] = category
                row["reviewed_subcategory"] = subcategory
            break
    save_json(queue_path, payload)


def review_open_target(product):
    retailer = normalize_key(product.get("retailer"))
    if retailer == "target":
        return product.get("image"), "Target image"
    return product.get("url") or product.get("image"), "product page"


def front_browser_name():
    script = '''
    tell application "System Events"
      set frontApp to name of first application process whose frontmost is true
    end tell
    return frontApp
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            check=False,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return None


def close_front_browser_tab(app_name):
    if app_name == "Safari":
        script = 'tell application "Safari" to if (count of windows) > 0 then close current tab of front window'
    elif app_name == "Google Chrome":
        script = 'tell application "Google Chrome" to if (count of windows) > 0 then close active tab of front window'
    elif app_name == "Chromium":
        script = 'tell application "Chromium" to if (count of windows) > 0 then close active tab of front window'
    else:
        return False

    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True, text=True)
        return True
    except Exception:
        return False


def open_temporarily(url, *, seconds=3):
    if not url:
        return False
    try:
        subprocess.run(["open", url], check=False)
        time.sleep(max(0, seconds))
        app_name = front_browser_name()
        if app_name:
            close_front_browser_tab(app_name)
        return True
    except Exception as exc:
        print(f"Could not auto-open product: {exc}")
        return False


def maybe_auto_open_product(product, args):
    if not args.auto_open:
        return
    url, label = review_open_target(product)
    if not url:
        return
    print(f"Auto-opening {label} for {args.open_seconds:g}s...")
    open_temporarily(url, seconds=args.open_seconds)


def open_image(url):
    if not url:
        print("No image URL for this item.")
        return
    try:
        subprocess.run(["open", url], check=False)
    except Exception as exc:
        print(f"Could not open image: {exc}")


def review_items(args):
    taxonomy = build_fixed_taxonomy()
    category_names, subcategories = taxonomy_maps(taxonomy)
    queue_payload = load_json(args.queue, {"items": []})
    clip_lookup = build_clip_lookup(args.vision_audit)
    items = queue_items_from_payload(queue_payload, clip_lookup)[: args.limit]
    gold_payload = load_json(args.gold_labels, {"labels": []})
    labels_by_key = index_existing_labels(gold_payload)
    reviewed = 0
    skipped = 0

    for index, item in enumerate(items, start=1):
        product = item.get("product") or {}
        key = product_label_key(product)
        if args.only_unlabeled and key in labels_by_key:
            continue
        opened_for_item = False

        while True:
            clear_screen(not args.no_clear)
            print_item(index, len(items), item, labels_by_key.get(key))
            if not opened_for_item:
                maybe_auto_open_product(product, args)
                opened_for_item = True
            suggestions = suggested_pair(item)
            if suggestions:
                print("Suggestions:")
                for suggestion_index, (source, category, subcategory) in enumerate(suggestions, start=1):
                    print(f"  {suggestion_index}. {source}: {category} / {subcategory}")
            print()
            print("Commands: Enter=accept first suggestion, number/name=choose category, i=open image, o=open product, s=skip, q=quit")
            category_default = suggestions[0][1] if suggestions else None
            raw = input(f"Category [Enter={category_default or 'choose'}]: ").strip()
            command = raw.lower()
            if command in {"q", "quit", "exit"}:
                raise KeyboardInterrupt
            if command in {"s", "skip"}:
                skipped += 1
                break
            if command in {"i", "image", "open"}:
                open_image(product.get("image"))
                input("Press Enter to continue...")
                continue
            if command in {"o", "product", "url"}:
                url, _ = review_open_target(product)
                open_temporarily(url, seconds=args.open_seconds)
                input("Press Enter to continue...")
                continue
            if not raw and suggestions:
                category, subcategory = suggestions[0][1], suggestions[0][2]
            elif raw.isdigit() and suggestions and 1 <= int(raw) <= len(suggestions):
                _, category, subcategory = suggestions[int(raw) - 1]
            else:
                print()
                display_options(category_names)
                category = choose_option("Category", category_names, allow_skip=True)
                if category is None:
                    skipped += 1
                    break
                print()
                display_options(subcategories[category])
                subcategory = choose_option("Subcategory", subcategories[category], allow_skip=True)
                if subcategory is None:
                    skipped += 1
                    break

            normalized = valid_label(category, subcategory, taxonomy)
            if not normalized:
                print(f"Invalid taxonomy pair: {category} / {subcategory}")
                input("Press Enter to retry...")
                continue

            print(f"\nSave gold label: {normalized['category']} / {normalized['subcategory']} ?")
            confirm = input("[Y/n/edit]: ").strip().lower()
            if confirm in {"n", "no"}:
                skipped += 1
                break
            if confirm in {"e", "edit"}:
                continue

            label = make_label(item, normalized["category"], normalized["subcategory"], taxonomy)
            labels_by_key[label_key(label)] = label
            save_gold_labels(args.gold_labels, labels_by_key)
            update_review_queue(args.queue, item, normalized["category"], normalized["subcategory"])
            reviewed += 1
            print(f"Saved. Gold labels: {len(labels_by_key)}")
            break

    save_gold_labels(args.gold_labels, labels_by_key)
    print(f"\nReview session complete. Saved={reviewed}, skipped={skipped}, total gold labels={len(labels_by_key)}")


def parse_args():
    parser = argparse.ArgumentParser(description="Interactively review taxonomy labels into the gold set.")
    parser.add_argument("--queue", default="taxonomy_review_queue.json")
    parser.add_argument("--gold-labels", default="taxonomy_gold_labels.json")
    parser.add_argument("--vision-audit", default="vision_category_audit.full.json")
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--only-unlabeled", action="store_true", default=True)
    parser.add_argument("--include-labeled", dest="only_unlabeled", action="store_false")
    parser.add_argument("--no-clear", action="store_true")
    parser.add_argument("--auto-open", action="store_true", default=True)
    parser.add_argument("--no-auto-open", dest="auto_open", action="store_false")
    parser.add_argument("--open-seconds", type=float, default=3.0)
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        review_items(args)
    except KeyboardInterrupt:
        print("\nStopped. Progress was autosaved.")


if __name__ == "__main__":
    main()
