#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

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


def identity_values(product):
    values = set()
    for field in ("asin", "url", "raw_name", "name", "id"):
        value = product.get(field)
        if value:
            values.add(f"{field}:{normalize_key(value)}")
    for value in product.get("asins") or []:
        if value:
            values.add(f"asin:{normalize_key(value)}")
    return values


def index_labels(rows):
    index = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for value in identity_values(row):
            index[value] = row
    return index


def has_label(product, label_index):
    return any(value in label_index for value in identity_values(product))


def taxonomy_pairs(taxonomy):
    pairs = []
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        for subcategory in category.get("subcategories") or []:
            subcategory_name = subcategory.get("name")
            if category_name and subcategory_name:
                pairs.append((category_name, subcategory_name))
    return pairs


def taxonomy_text(taxonomy):
    lines = []
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        subcategories = ", ".join(
            subcategory.get("name")
            for subcategory in category.get("subcategories") or []
            if subcategory.get("name")
        )
        lines.append(f"- {category_name}: {subcategories}")
    return "\n".join(lines)


def compact_product(product, index, clip=None):
    return {
        "id": str(index),
        "asin": product.get("asin"),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "brand": product.get("brand") or product.get("source_brand"),
        "retailer": product.get("retailer"),
        "sources": product.get("sources") or [],
        "source_categories": product.get("source_categories") or [],
        "failed_from_category": product.get("failed_from_category"),
        "failed_from_subcategory": product.get("failed_from_subcategory"),
        "current_category": product.get("category"),
        "current_subcategory": product.get("subcategory"),
        "clip": clip,
    }


def product_summary_for_label(product):
    return {
        "asin": product.get("asin"),
        "url": product.get("url"),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "brand": product.get("brand") or product.get("source_brand"),
        "retailer": product.get("retailer"),
    }


def clip_lookup(clip_report):
    lookup = {}
    for row in clip_report.get("results") or []:
        product = row.get("product") or {}
        locked = row.get("clip_locked_pair") or {}
        if not locked.get("category") or not locked.get("subcategory"):
            continue
        clip = {
            "category": locked.get("category"),
            "subcategory": locked.get("subcategory"),
            "score": locked.get("score"),
            "source": locked.get("source"),
        }
        for value in identity_values(product):
            lookup[value] = clip
    return lookup


def clip_for_product(product, lookup):
    for value in identity_values(product):
        clip = lookup.get(value)
        if clip:
            return clip
    return None


def select_products(products, gold_index, silver_index, clip_index, *, only_failed, limit):
    selected = []
    for product in products:
        if only_failed and not (
            product.get("classification_status") == "failed"
            or product.get("category") == "Other/Failed"
            or product.get("subcategory") == "Needs Review"
        ):
            continue
        if has_label(product, gold_index) or has_label(product, silver_index):
            continue
        selected.append((product, clip_for_product(product, clip_index)))
        if limit and len(selected) >= limit:
            break
    return selected


def response_schema():
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["labels"],
        "properties": {
            "labels": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "id",
                        "category",
                        "subcategory",
                        "confidence",
                        "reason",
                        "needs_human_review",
                    ],
                    "properties": {
                        "id": {"type": "string"},
                        "category": {"type": "string"},
                        "subcategory": {"type": "string"},
                        "confidence": {"type": "number"},
                        "reason": {"type": "string"},
                        "needs_human_review": {"type": "boolean"},
                    },
                },
            }
        },
    }


def build_prompt(batch, taxonomy):
    products = [
        compact_product(product, index, clip=clip)
        for index, (product, clip) in enumerate(batch, start=1)
    ]
    return (
        "Classify each grocery deal into exactly one category/subcategory pair from the taxonomy.\n"
        "Use product name, raw name, brand, retailer, source, and CLIP hint if present.\n"
        "The CLIP hint is useful but not authoritative. Gold/manual labels are not shown here.\n"
        "Do not invent categories or subcategories. If uncertain, choose the best valid pair and set "
        "needs_human_review=true with lower confidence.\n\n"
        "Taxonomy:\n"
        f"{taxonomy_text(taxonomy)}\n\n"
        "Products:\n"
        f"{json.dumps(products, indent=2, ensure_ascii=False)}"
    )


def manual_prompt_text(batch, taxonomy, *, chunk_index, chunk_count):
    return (
        f"You are classifying grocery deals. This is chunk {chunk_index} of {chunk_count}.\n\n"
        f"{build_prompt(batch, taxonomy)}\n\n"
        "Return ONLY valid JSON in this exact shape, with one label per product id:\n"
        "{\n"
        "  \"labels\": [\n"
        "    {\n"
        "      \"id\": \"1\",\n"
        "      \"category\": \"Pantry\",\n"
        "      \"subcategory\": \"Pasta Sauces\",\n"
        "      \"confidence\": 0.86,\n"
        "      \"reason\": \"short reason\",\n"
        "      \"needs_human_review\": false\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )


def write_manual_prompt_files(selected, taxonomy, *, output_dir, batch_size):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_size = max(1, batch_size)
    batches = list(chunks(selected, batch_size))
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_size": batch_size,
        "chunk_count": len(batches),
        "product_count": len(selected),
        "chunks": [],
    }
    for index, batch in enumerate(batches, start=1):
        prompt_path = output_dir / f"silver_prompt_{index:03}.txt"
        response_path = output_dir / f"silver_response_{index:03}.json"
        prompt_path.write_text(
            manual_prompt_text(batch, taxonomy, chunk_index=index, chunk_count=len(batches)),
            encoding="utf-8",
        )
        manifest["chunks"].append(
            {
                "index": index,
                "prompt": str(prompt_path),
                "response": str(response_path),
                "product_count": len(batch),
            }
        )
    save_json(output_dir / "manifest.json", manifest)
    return manifest


def load_manual_response(path):
    text = Path(path).read_text(encoding="utf-8").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return json.loads(text)


def extract_output_text(payload):
    if isinstance(payload, dict) and payload.get("output_text"):
        return payload["output_text"]
    texts = []
    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                texts.append(content["text"])
    return "\n".join(texts)


def rate_limit_sleep_seconds(response, attempt, base_delay):
    retry_after = response.headers.get("retry-after") if response is not None else None
    if retry_after:
        try:
            return max(float(retry_after), base_delay)
        except ValueError:
            pass
    return base_delay * (2 ** attempt)


def call_openai(prompt, *, api_key, model, base_url, timeout, retries, retry_delay):
    payload = {
        "model": model,
        "instructions": (
            "You are a careful grocery taxonomy classifier. Return only labels that use "
            "the provided fixed taxonomy exactly."
        ),
        "input": prompt,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "taxonomy_silver_backfill",
                "strict": True,
                "schema": response_schema(),
            }
        },
    }
    response = None
    for attempt in range(retries + 1):
        response = requests.post(
            f"{base_url.rstrip('/')}/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
        if response.status_code not in {408, 409, 429, 500, 502, 503, 504}:
            break
        if attempt >= retries:
            break
        sleep_seconds = rate_limit_sleep_seconds(response, attempt, retry_delay)
        print(
            f"[silver] OpenAI HTTP {response.status_code}; "
            f"retrying in {sleep_seconds:.1f}s ({attempt + 1}/{retries})"
        )
        time.sleep(sleep_seconds)

    if response is None:
        raise RuntimeError("OpenAI request was not attempted.")
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:2000] if response is not None else ""
        raise RuntimeError(f"OpenAI request failed after retries: {exc}\n{body}") from exc
    text = extract_output_text(response.json())
    if not text:
        raise RuntimeError(f"OpenAI response did not include output text: {response.text[:1000]}")
    return json.loads(text)


def validate_label(product, raw_label, taxonomy):
    normalized = normalize_model_result(
        {
            "category": raw_label.get("category"),
            "subcategory": raw_label.get("subcategory"),
            "confidence": raw_label.get("confidence") or 0.8,
            "reasoning": raw_label.get("reason") or "OpenAI silver taxonomy backfill label.",
        },
        taxonomy,
    )
    if not normalized:
        return None
    confidence = max(0.55, min(float(raw_label.get("confidence") or 0.8), 0.92))
    return {
        **product_summary_for_label(product),
        "category": normalized["category"],
        "subcategory": normalized["subcategory"],
        "confidence": round(confidence, 4),
        "reason": raw_label.get("reason") or "OpenAI silver taxonomy backfill label.",
        "needs_human_review": bool(raw_label.get("needs_human_review")),
        "source": "openai-silver-backfill",
    }


def merge_labels(existing_payload, new_labels, *, model):
    existing = existing_payload.get("labels") if isinstance(existing_payload, dict) else existing_payload
    existing = list(existing or [])
    by_key = {}
    for row in existing:
        keys = identity_values(row)
        if keys:
            by_key[sorted(keys)[0]] = row
    for row in new_labels:
        keys = identity_values(row)
        if keys:
            by_key[sorted(keys)[0]] = row
    labels = sorted(by_key.values(), key=lambda row: (row.get("retailer") or "", row.get("brand") or "", row.get("name") or ""))
    return {
        "instructions": (
            "AI-generated silver taxonomy labels. These are below manual gold labels and source-backed "
            "rules, but above CLIP/local fallback, and can be replaced by manual review anytime."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "label_count": len(labels),
        "labels": labels,
    }


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill failed taxonomy labels with OpenAI silver labels.")
    parser.add_argument("--products", default="combined_products.json")
    parser.add_argument("--gold-labels", default="taxonomy_gold_labels.json")
    parser.add_argument("--silver-labels", default="taxonomy_silver_labels.json")
    parser.add_argument("--vision-audit", default="vision_category_audit.full.json")
    parser.add_argument("--only-failed", action="store_true", default=True)
    parser.add_argument("--include-all", dest="only_failed", action="store_false")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-5.4-mini"))
    parser.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"))
    parser.add_argument("--api-key-env", default="OPENAI_API_KEY")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--retry-delay", type=float, default=20.0)
    parser.add_argument("--sleep-between-batches", type=float, default=5.0)
    parser.add_argument("--write-manual-prompts", default=None)
    parser.add_argument("--import-manual-responses", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    products = load_json(args.products, [])
    taxonomy = build_fixed_taxonomy()
    gold_payload = load_json(args.gold_labels, {"labels": []})
    silver_payload = load_json(args.silver_labels, {"labels": []})
    clip_report = load_json(args.vision_audit, {"results": []})
    gold_rows = gold_payload.get("labels") if isinstance(gold_payload, dict) else gold_payload
    silver_rows = silver_payload.get("labels") if isinstance(silver_payload, dict) else silver_payload
    gold_index = index_labels(gold_rows or [])
    silver_index = index_labels(silver_rows or [])
    clips = clip_lookup(clip_report)
    selected = select_products(
        products,
        gold_index,
        silver_index,
        clips,
        only_failed=args.only_failed,
        limit=args.limit,
    )
    print(f"Selected {len(selected)} products for silver backfill.")
    if not selected:
        return

    if args.dry_run:
        preview = build_prompt(selected[: min(len(selected), args.batch_size)], taxonomy)
        print(preview[:12000])
        return

    if args.write_manual_prompts:
        manifest = write_manual_prompt_files(
            selected,
            taxonomy,
            output_dir=args.write_manual_prompts,
            batch_size=args.batch_size,
        )
        print(
            f"Wrote {manifest['chunk_count']} manual prompt chunks "
            f"for {manifest['product_count']} products to {args.write_manual_prompts}"
        )
        print("Paste each prompt into ChatGPT, save each JSON response to the matching silver_response_###.json file, then run with --import-manual-responses.")
        return

    if args.import_manual_responses:
        manifest = load_json(Path(args.import_manual_responses) / "manifest.json", {})
        chunks_info = manifest.get("chunks") or []
        new_labels = []
        selected_batches = list(chunks(selected, max(1, args.batch_size)))
        for info, batch in zip(chunks_info, selected_batches):
            response_path = Path(info["response"])
            if not response_path.exists():
                print(f"[silver] missing manual response: {response_path}")
                continue
            payload = load_manual_response(response_path)
            labels_by_id = {
                str(label.get("id")): label
                for label in payload.get("labels") or []
                if isinstance(label, dict)
            }
            for product_index, (product, _clip) in enumerate(batch, start=1):
                raw_label = labels_by_id.get(str(product_index))
                if not raw_label:
                    print(f"[silver] missing manual label for {product.get('name')}")
                    continue
                label = validate_label(product, raw_label, taxonomy)
                if not label:
                    print(f"[silver] invalid manual taxonomy pair for {product.get('name')}: {raw_label}")
                    continue
                new_labels.append(label)
        merged = merge_labels(silver_payload, new_labels, model="manual-chatgpt-import")
        save_json(args.silver_labels, merged)
        print(f"Wrote {args.silver_labels} with {len(merged['labels'])} silver labels ({len(new_labels)} imported).")
        return

    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise SystemExit(f"Missing {args.api_key_env}. Set it before running this script.")

    new_labels = []
    for batch_index, batch in enumerate(chunks(selected, max(1, args.batch_size)), start=1):
        print(f"[silver] batch {batch_index}: {len(batch)} products")
        prompt = build_prompt(batch, taxonomy)
        payload = call_openai(
            prompt,
            api_key=api_key,
            model=args.model,
            base_url=args.base_url,
            timeout=args.timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
        )
        labels_by_id = {
            str(label.get("id")): label
            for label in payload.get("labels") or []
            if isinstance(label, dict)
        }
        for product_index, (product, _clip) in enumerate(batch, start=1):
            raw_label = labels_by_id.get(str(product_index))
            if not raw_label:
                print(f"[silver] missing label for {product.get('name')}")
                continue
            label = validate_label(product, raw_label, taxonomy)
            if not label:
                print(f"[silver] invalid taxonomy pair for {product.get('name')}: {raw_label}")
                continue
            new_labels.append(label)
        merged = merge_labels(silver_payload, new_labels, model=args.model)
        save_json(args.silver_labels, merged)
        if args.sleep_between_batches:
            time.sleep(max(0, args.sleep_between_batches))

    merged = merge_labels(silver_payload, new_labels, model=args.model)
    save_json(args.silver_labels, merged)
    print(f"Wrote {args.silver_labels} with {len(merged['labels'])} silver labels ({len(new_labels)} new).")


if __name__ == "__main__":
    main()
