import base64
import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests


OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
MODEL_VERSION = "taxonomy-ai-v1"
PROMPT_VERSION = "taxonomy-prompt-v1"
DISCOVERED_TAXONOMY_FILE = "discovered_taxonomy.json"
CLASSIFICATION_CACHE_FILE = "taxonomy_classification_cache.json"
TAXONOMY_REPORT_FILE = "taxonomy_ai_report.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value):
    if not value:
        return ""
    return " ".join(str(value).strip().split())


def slugify(value):
    text = normalize_text(value).lower()
    cleaned = []
    dash = False
    for ch in text:
        if ch.isalnum():
            cleaned.append(ch)
            dash = False
        elif not dash:
            cleaned.append("-")
            dash = True
    return "".join(cleaned).strip("-") or "unknown"


def ollama_available(timeout=2):
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=timeout)
        return response.ok
    except Exception:
        return False


def fetch_image_as_base64(url, timeout=10):
    if not url:
        return None
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("image/"):
            return None
        return base64.b64encode(response.content).decode("utf-8")
    except Exception:
        return None


def extract_json_block(text):
    if not text:
        return None
    start = text.find("{")
    if start == -1:
        return None
    try:
        decoder = json.JSONDecoder()
        parsed, _ = decoder.raw_decode(text[start:])
        return parsed
    except Exception:
        return None


def chat_json(*, system, prompt, images=None, timeout=120, retries=2):
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "format": "json",
        "messages": [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": prompt,
                **({"images": images} if images else {}),
            },
        ],
    }
    last_content = ""
    for attempt in range(retries + 1):
        response = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        content = ((body.get("message") or {}).get("content")) or ""
        last_content = content
        parsed = extract_json_block(content)
        if parsed is not None:
            return parsed
        if attempt < retries:
            payload["messages"].append(
                {
                    "role": "user",
                    "content": (
                        "Your previous reply was not valid standalone JSON. "
                        "Reply again with JSON only and no extra text before or after the object."
                    ),
                }
            )
    raise ValueError(f"Could not parse JSON from Ollama response: {last_content[:400]}")


def build_product_summary(product):
    return {
        "id": product.get("asin") or product.get("url") or product.get("name"),
        "retailer": product.get("retailer"),
        "brand": product.get("brand"),
        "name": product.get("name"),
        "raw_name": product.get("raw_name"),
        "source_categories": product.get("source_categories") or [],
        "sources": product.get("sources") or [],
        "url": product.get("url"),
        "image_url": product.get("image"),
    }


def taxonomy_to_options(taxonomy):
    options = {}
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        options[category_name] = {}
        for subcategory in category.get("subcategories") or []:
            options[category_name][subcategory.get("name")] = []
    return options


def taxonomy_pairs(taxonomy):
    pairs = []
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        for subcategory in category.get("subcategories") or []:
            pairs.append({"category": category_name, "subcategory": subcategory.get("name")})
    return pairs


def discover_taxonomy(products, chunk_size=150):
    if not ollama_available():
        raise RuntimeError("Ollama is not available for taxonomy discovery.")

    system = (
        "You are designing a grocery taxonomy across Whole Foods, Target, and H Mart. "
        "Return JSON only. Create a practical consumer-facing taxonomy with top-level categories "
        "and subcategories. Keep labels stable, short, and useful for a shopping app. "
        "Do not create retailer-specific categories."
    )

    chunk_taxonomies = []
    summaries = [build_product_summary(product) for product in products]
    for start in range(0, len(summaries), chunk_size):
        chunk = summaries[start : start + chunk_size]
        prompt = (
            "Here is a chunk of grocery products from multiple retailers.\n"
            "Infer a shopper-friendly taxonomy from them.\n"
            "Return JSON with shape {\"categories\":[{\"name\":...,\"subcategories\":[{\"name\":...}]}]}.\n"
            f"Products:\n{json.dumps(chunk, ensure_ascii=False)}"
        )
        chunk_taxonomies.append(chat_json(system=system, prompt=prompt, timeout=300))

    merge_prompt = (
        "Merge these candidate taxonomies into one final stable taxonomy for a grocery deals app.\n"
        "Return JSON with shape {\"categories\":[{\"name\":...,\"subcategories\":[{\"name\":...}]}]}.\n"
        f"Candidate taxonomies:\n{json.dumps(chunk_taxonomies, ensure_ascii=False)}"
    )
    merged = chat_json(system=system, prompt=merge_prompt, timeout=300)
    taxonomy = {
        "taxonomy_version": f"taxonomy-{int(time.time())}",
        "model_name": OLLAMA_MODEL,
        "model_version": MODEL_VERSION,
        "prompt_version": PROMPT_VERSION,
        "discovery_mode": "ollama",
        "generated_at": utc_now(),
        "categories": [],
    }
    for category in merged.get("categories") or []:
        category_name = normalize_text(category.get("name"))
        if not category_name:
            continue
        subcategories = []
        seen = set()
        for subcategory in category.get("subcategories") or []:
            sub_name = normalize_text(subcategory.get("name") if isinstance(subcategory, dict) else subcategory)
            if not sub_name or sub_name in seen:
                continue
            seen.add(sub_name)
            subcategories.append({"name": sub_name, "slug": slugify(sub_name)})
        if subcategories:
            taxonomy["categories"].append(
                {
                    "name": category_name,
                    "slug": slugify(category_name),
                    "subcategories": subcategories,
                }
            )
    return taxonomy


def build_taxonomy_artifact(base_dir):
    return os.path.join(base_dir, DISCOVERED_TAXONOMY_FILE)


def build_cache_artifact(base_dir):
    return os.path.join(base_dir, CLASSIFICATION_CACHE_FILE)


def build_report_artifact(base_dir):
    return os.path.join(base_dir, TAXONOMY_REPORT_FILE)


def load_json_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def save_json_file(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def product_fingerprint(product, taxonomy_version):
    payload = {
        "raw_name": product.get("raw_name") or product.get("name"),
        "name": product.get("name"),
        "brand": product.get("brand"),
        "retailer": product.get("retailer"),
        "source_categories": product.get("source_categories") or [],
        "sources": product.get("sources") or [],
        "url": product.get("url"),
        "image": product.get("image"),
        "taxonomy_version": taxonomy_version,
        "prompt_version": PROMPT_VERSION,
        "model_name": OLLAMA_MODEL,
        "model_version": MODEL_VERSION,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return digest


def classify_one_product(product, taxonomy):
    if not ollama_available():
        raise RuntimeError("Ollama is not available for product classification.")

    taxonomy_pairs_text = [
        f"- {pair['category']} -> {pair['subcategory']}"
        for pair in taxonomy_pairs(taxonomy)
    ]
    system = (
        "You classify grocery products for a shopping app. "
        "Return JSON only with keys: category, subcategory, confidence, reasoning. "
        "Pick the best category and subcategory from the provided taxonomy list. "
        "Reason over the whole product, including retailer metadata and image."
    )
    prompt = (
        "Choose the best category/subcategory for this grocery product.\n"
        f"Available taxonomy pairs:\n{chr(10).join(taxonomy_pairs_text)}\n\n"
        f"Product:\n{json.dumps(build_product_summary(product), ensure_ascii=False)}"
    )
    image_b64 = fetch_image_as_base64(product.get("image"))
    result = chat_json(system=system, prompt=prompt, images=[image_b64] if image_b64 else None, timeout=180)
    category = normalize_text(result.get("category"))
    subcategory = normalize_text(result.get("subcategory"))
    confidence = float(result.get("confidence") or 0)
    reasoning = normalize_text(result.get("reasoning"))
    return {
        "category": category,
        "subcategory": subcategory,
        "confidence": max(0.0, min(confidence, 1.0)),
        "reasoning": reasoning,
        "model_name": OLLAMA_MODEL,
        "model_version": MODEL_VERSION,
    }


def taxonomy_is_bootstrap(taxonomy):
    if not taxonomy:
        return False
    taxonomy_version = str(taxonomy.get("taxonomy_version") or "")
    model_name = str(taxonomy.get("model_name") or "")
    discovery_mode = str(taxonomy.get("discovery_mode") or "")
    return (
        taxonomy_version.startswith("bootstrap-")
        or model_name == "bootstrap-existing-catalog"
        or discovery_mode == "bootstrap"
    )


def classification_is_bootstrap(result):
    if not result:
        return False
    return str(result.get("model_name") or "") == "bootstrap-existing-catalog"


def ensure_taxonomy(base_dir, products, force_rediscover=False):
    path = build_taxonomy_artifact(base_dir)
    taxonomy = load_json_file(path, None)
    if taxonomy_is_bootstrap(taxonomy):
        taxonomy = None
    if taxonomy and not force_rediscover:
        return taxonomy

    if not ollama_available():
        raise RuntimeError(
            "Ollama is required to discover the taxonomy. Start Ollama and rerun "
            "`python3 refresh_and_post_results.py --rediscover-taxonomy`."
        )

    taxonomy = discover_taxonomy(products)
    save_json_file(path, taxonomy)
    return taxonomy


def classify_products(base_dir, products, force_rediscover=False):
    taxonomy = ensure_taxonomy(base_dir, products, force_rediscover=force_rediscover)
    cache_path = build_cache_artifact(base_dir)
    report_path = build_report_artifact(base_dir)
    cache = load_json_file(cache_path, {"taxonomy_version": taxonomy.get("taxonomy_version"), "items": {}})
    if cache.get("taxonomy_version") != taxonomy.get("taxonomy_version"):
        cache = {"taxonomy_version": taxonomy.get("taxonomy_version"), "items": {}}

    changed = []
    updated_products = []
    for product in products:
        fingerprint = product_fingerprint(product, taxonomy.get("taxonomy_version"))
        cached = (cache.get("items") or {}).get(fingerprint)
        if cached and not classification_is_bootstrap(cached):
            result = dict(cached)
            result["cache_hit"] = True
        else:
            if not ollama_available():
                raise RuntimeError(
                    "Ollama is required to classify products without a cached model result. "
                    "Start Ollama and rerun the refresh."
                )
            result = classify_one_product(product, taxonomy)
            cache.setdefault("items", {})[fingerprint] = result
            changed.append(product.get("raw_name") or product.get("name"))

        updated = dict(product)
        updated["category"] = result.get("category") or updated.get("category") or "Uncategorized"
        updated["subcategory"] = result.get("subcategory") or updated.get("subcategory") or "Miscellaneous"
        updated["category_confidence"] = round(float(result.get("confidence") or 0), 4)
        updated["ai_category"] = updated["category"]
        updated["ai_subcategory"] = updated["subcategory"]
        updated["ai_confidence"] = round(float(result.get("confidence") or 0), 4)
        updated["ai_reasoning"] = result.get("reasoning") or ""
        updated["ai_model_name"] = result.get("model_name") or OLLAMA_MODEL
        updated["ai_model_version"] = result.get("model_version") or MODEL_VERSION
        updated["ai_taxonomy_version"] = taxonomy.get("taxonomy_version")
        updated["ai_fingerprint"] = fingerprint
        updated["ai_label_source"] = "model"
        updated_products.append(updated)

    save_json_file(cache_path, cache)
    save_json_file(
        report_path,
        {
            "generated_at": utc_now(),
            "taxonomy_version": taxonomy.get("taxonomy_version"),
            "model_name": taxonomy.get("model_name"),
            "model_version": MODEL_VERSION,
            "product_count": len(updated_products),
            "changed_count": len(changed),
            "changed_examples": changed[:200],
        },
    )
    return updated_products, taxonomy
