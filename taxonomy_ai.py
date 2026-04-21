import base64
import hashlib
import json
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests
from requests.exceptions import ReadTimeout
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import FeatureUnion
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from fixed_taxonomy import FIXED_TAXONOMY_VERSION, build_fixed_taxonomy

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
MODEL_VERSION = "taxonomy-local-ml-v16"
PROMPT_VERSION = f"taxonomy-prompt-{FIXED_TAXONOMY_VERSION}-local-ml-v16"
OLLAMA_CHAT_TIMEOUT = int(os.getenv("OLLAMA_CHAT_TIMEOUT", "420"))
CLASSIFICATION_BATCH_SIZE = int(os.getenv("TAXONOMY_CLASSIFICATION_BATCH_SIZE", "20"))
DISCOVERED_TAXONOMY_FILE = "discovered_taxonomy.json"
CLASSIFICATION_CACHE_FILE = "taxonomy_classification_cache.json"
TAXONOMY_REPORT_FILE = "taxonomy_ai_report.json"
DISCOVERY_DEBUG_FILE = "taxonomy_discovery_debug.json"
CLASSIFICATION_DEBUG_FILE = "taxonomy_classification_debug.json"
GOLD_LABELS_FILE = "taxonomy_gold_labels.json"
SILVER_LABELS_FILE = "taxonomy_silver_labels.json"
CLIP_AUDIT_FILE = "vision_category_audit.full.json"
CLIP_PRIMARY_MIN_SCORE = 0.55
CLIP_RESCUE_MIN_SCORE = 0.40
FAILED_CATEGORY = "Other/Failed"
FAILED_SUBCATEGORY = "Needs Review"
FAILED_CONFIDENCE_MAX = 0.35


CATEGORY_GUIDANCE = {
    "Produce": "fresh fruits, fresh vegetables, herbs, mushrooms, cut produce; not bottled drinks, sauces, supplements, or shelf-stable tomato products",
    "Meat & Seafood": "raw or cooked animal proteins, deli meats, sausages, seafood, and true meat alternatives; not collagen pills, beverages, or pantry sauces",
    "Dairy & Eggs": "milk, cheese, yogurt, eggs, butter, creamers, and dairy alternatives",
    "Bakery": "bread, bagels, tortillas, pastries, cakes, pies, and bakery-made sweets; packaged snack cookies usually belong in Snacks",
    "Pantry": "shelf-stable cooking staples, pasta, grains, canned goods, sauces, condiments, oils, baking supplies, spreads, and meal kits",
    "Snacks": "chips, crackers, cookies, candy, bars, popcorn, jerky, fruit snacks, and other ready-to-eat snack foods; not supplements just because they are gummies",
    "Beverages": "non-alcoholic drinks and drink-making products such as water, juice, soda, coffee, tea, kombucha, sports drinks, coffee beans, pods, concentrates, and drink mixes",
    "Alcohol": "beer, wine, spirits, hard seltzer, cider, canned cocktails, and non-alcoholic beer or wine; alcoholic drinks should not use Beverages",
    "Frozen": "products sold frozen, including frozen meals, frozen vegetables, frozen fruit, frozen pizza, ice cream, and frozen appetizers",
    "Prepared Foods": "ready-to-eat or refrigerated prepared meals, soups, salads, sushi, kimchi, tofu, plant-based proteins, refrigerated noodles, and deli-style foods",
    "International": "region-specific foods when the regional identity is the main organizing feature, especially Asian, Korean, Japanese, Indian, Hispanic, Mediterranean, and Middle Eastern items",
    "Supplements & Wellness": "vitamins, minerals, probiotics, protein, collagen, herbal supplements, sleep/stress/digestive/immune support, hydration tablets, wellness shots, and essential oils",
    "Beauty & Personal Care": "skin care, hair care, body care, oral care, deodorant, soap, cosmetics, sunscreen, lip care, shaving, and grooming",
    "Household": "cleaning, dishwashing, laundry, paper goods, trash bags, foil, wraps, bags, food storage, kitchen supplies, coffee filters, insect repellent, and home essentials",
    "Baby": "baby food, formula, diapers, wipes, baby snacks, baby care, and baby wellness",
}


GLOBAL_CLASSIFICATION_RULES = [
    "Classify what the product is, not what an ingredient resembles.",
    "If the display name is generic, short, or size-only, rely on authoritative_name, brand, breadcrumbs, URL, and image.",
    "Fresh Produce is only for whole or minimally prepared fresh produce; packaged cereal, snacks, pouches, powders, sauces, and supplements should never become Produce because of flavor or ingredient words.",
    "Alcoholic beer, wine, spirits, hard seltzer, cider, and canned cocktails belong in Alcohol, not Beverages.",
    "Non-food household and personal-care products should not be forced into food categories.",
    "Essential oils and aromatherapy oils usually belong in Supplements & Wellness > Essential Oils unless the product is clearly a skin, hair, body, or cosmetic treatment.",
    "Coffee beans, ground coffee, coffee pods, K-Cups, and coffee concentrate are Beverages, but only bottled or canned coffee should use Ready-to-Drink Coffee.",
    "Tofu and plain plant-based protein products are not meat, dairy, or produce.",
]


SUBCATEGORY_GUIDANCE = {
    ("Beverages", "Coffee"): "broad coffee products when no more specific coffee subcategory fits",
    ("Beverages", "Coffee Beans & Grounds"): "whole bean coffee, ground coffee, roast coffee, and loose coffee grounds",
    ("Beverages", "Coffee Pods & K-Cups"): "single-serve coffee pods, capsules, K-Cups, and Nespresso-compatible pods",
    ("Beverages", "Coffee Concentrates"): "cold brew concentrate and coffee concentrate that must be diluted or mixed",
    ("Beverages", "Ready-to-Drink Coffee"): "bottled, canned, or carton coffee drinks that are ready to drink as sold",
    ("Beverages", "Drink Mixes"): "powders, tablets, sticks, or drops used to make non-alcoholic drinks",
    ("Pantry", "Cereal & Breakfast"): "boxed or bagged breakfast cereal, granola, oatmeal, toaster pastries, and shelf-stable breakfast foods",
    ("Alcohol", "Beer"): "beer, lager, IPA, ale, stout, porter, and beer multipacks",
    ("Alcohol", "Non-Alcoholic Beer & Wine"): "non-alcoholic beer, hop water, hoppy refresher, and alcohol-free wine",
    ("Prepared Foods", "Tofu & Plant-Based Proteins"): "plain tofu, seasoned tofu, tempeh, seitan, and refrigerated plant-based proteins",
    ("Household", "Foil, Wrap & Bags"): "aluminum foil, parchment, plastic wrap, wax paper, sandwich bags, and food wrap",
    ("Household", "Kitchen Supplies"): "coffee filters, food prep supplies, disposable kitchen tools, and kitchen utility items",
    ("Household", "Insect Repellent"): "bug spray, mosquito repellent, insect repellent, and pest-repelling personal sprays",
    ("Supplements & Wellness", "Essential Oils"): "pure essential oils, aromatherapy blends, roll-ons, oil kits, and diffuser oils",
    ("Beauty & Personal Care", "Skin Care"): "facial care, moisturizers, serums, masks, acne care, and skin treatments",
}


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
    starts = [index for index in (text.find("{"), text.find("[")) if index != -1]
    if not starts:
        return None
    start = min(starts)
    try:
        decoder = json.JSONDecoder()
        parsed, _ = decoder.raw_decode(text[start:])
        return parsed
    except Exception:
        return None


def chat_json(*, system, prompt, images=None, timeout=120, retries=2, include_raw=False):
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
        try:
            response = requests.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=timeout)
        except ReadTimeout:
            if attempt < retries:
                continue
            raise
        response.raise_for_status()
        body = response.json()
        content = ((body.get("message") or {}).get("content")) or ""
        last_content = content
        parsed = extract_json_block(content)
        if parsed is not None:
            if include_raw:
                return parsed, content
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
        "variation": product.get("variation"),
        "current_price": product.get("current_price"),
        "basis_price": product.get("basis_price"),
        "prime_price": product.get("prime_price"),
        "tags": product.get("tags") or [],
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
    taxonomy = build_fixed_taxonomy()
    taxonomy["generated_at"] = utc_now()
    taxonomy["model_version"] = MODEL_VERSION
    taxonomy["prompt_version"] = PROMPT_VERSION
    return taxonomy


def build_taxonomy_artifact(base_dir):
    return os.path.join(base_dir, DISCOVERED_TAXONOMY_FILE)


def build_cache_artifact(base_dir):
    return os.path.join(base_dir, CLASSIFICATION_CACHE_FILE)


def build_report_artifact(base_dir):
    return os.path.join(base_dir, TAXONOMY_REPORT_FILE)


def build_discovery_debug_artifact(base_dir):
    return os.path.join(base_dir, DISCOVERY_DEBUG_FILE)


def build_classification_debug_artifact(base_dir):
    return os.path.join(base_dir, CLASSIFICATION_DEBUG_FILE)


def save_classification_progress(report_path, *, taxonomy, total_count, changed, completed_count, last_product=None):
    save_json_file(
        report_path,
        {
            "generated_at": utc_now(),
            "taxonomy_version": taxonomy.get("taxonomy_version"),
            "model_name": taxonomy.get("model_name"),
            "model_version": MODEL_VERSION,
            "product_count": total_count,
            "completed_count": completed_count,
            "changed_count": len(changed),
            "changed_examples": changed[:200],
            "last_product": last_product,
        },
    )


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
        "model_name": "local-taxonomy-classifier",
        "model_version": MODEL_VERSION,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return digest


def build_product_prompt_payload(product):
    payload = build_product_summary(product)
    raw_name = normalize_text(product.get("raw_name") or "")
    display_name = normalize_text(product.get("name") or "")
    payload["authoritative_name"] = raw_name or display_name
    payload["display_name"] = display_name
    payload["name_quality"] = (
        "abbreviated"
        if display_name in {"", ".", "1 each", "12 pk", "6 pk", "4 pks", "6 pks", "12 pks"}
        or len(display_name) <= 6
        else "normal"
    )
    return payload


def build_compact_product_payload(product, local_id):
    payload = build_product_prompt_payload(product)
    return {
        "id": str(local_id),
        "retailer": payload.get("retailer"),
        "brand": payload.get("brand"),
        "authoritative_name": payload.get("authoritative_name"),
        "display_name": payload.get("display_name"),
        "name_quality": payload.get("name_quality"),
        "variation": payload.get("variation"),
        "tags": payload.get("tags") or [],
        "source_categories": payload.get("source_categories") or [],
        "sources": payload.get("sources") or [],
        "url": payload.get("url"),
    }


def format_category_option(index, category):
    name = category.get("name")
    guidance = CATEGORY_GUIDANCE.get(name)
    if guidance:
        return f"{index}. {name} - {guidance}"
    return f"{index}. {name}"


def format_subcategory_option(index, category_name, subcategory):
    name = subcategory.get("name")
    guidance = SUBCATEGORY_GUIDANCE.get((category_name, name))
    if guidance:
        return f"{index}. {name} - {guidance}"
    return f"{index}. {name}"


def classification_rules_text():
    return "\n".join(f"- {rule}" for rule in GLOBAL_CLASSIFICATION_RULES)


def taxonomy_prompt_text(taxonomy):
    lines = []
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        guidance = CATEGORY_GUIDANCE.get(category_name)
        suffix = f" - {guidance}" if guidance else ""
        lines.append(f"{category_name}{suffix}")
        for subcategory in category.get("subcategories") or []:
            subcategory_name = subcategory.get("name")
            sub_guidance = SUBCATEGORY_GUIDANCE.get((category_name, subcategory_name))
            sub_suffix = f" - {sub_guidance}" if sub_guidance else ""
            lines.append(f"  - {subcategory_name}{sub_suffix}")
    return "\n".join(lines)


def taxonomy_pair_choices(taxonomy):
    choices = []
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        for subcategory in category.get("subcategories") or []:
            subcategory_name = subcategory.get("name")
            guidance = SUBCATEGORY_GUIDANCE.get((category_name, subcategory_name))
            suffix = f" - {guidance}" if guidance else ""
            choices.append(
                {
                    "category": category_name,
                    "subcategory": subcategory_name,
                    "text": f"{len(choices) + 1}. {category_name} > {subcategory_name}{suffix}",
                }
            )
    return choices


def normalized_label(value):
    return normalize_text(value).casefold()


def taxonomy_lookup(taxonomy):
    categories_by_norm = {}
    subcategories_by_category_norm = {}
    unique_subcategories_by_norm = {}
    duplicate_subcategories = set()
    for category in taxonomy.get("categories") or []:
        category_name = category.get("name")
        category_key = normalized_label(category_name)
        categories_by_norm[category_key] = category_name
        subcategories = {}
        for subcategory in category.get("subcategories") or []:
            subcategory_name = subcategory.get("name")
            subcategory_key = normalized_label(subcategory_name)
            subcategories[subcategory_key] = subcategory_name
            if subcategory_key in unique_subcategories_by_norm:
                duplicate_subcategories.add(subcategory_key)
            else:
                unique_subcategories_by_norm[subcategory_key] = (category_name, subcategory_name)
        subcategories_by_category_norm[category_key] = subcategories
    for subcategory_key in duplicate_subcategories:
        unique_subcategories_by_norm.pop(subcategory_key, None)
    return categories_by_norm, subcategories_by_category_norm, unique_subcategories_by_norm


def normalize_model_result(result, taxonomy):
    categories_by_norm, subcategories_by_category_norm, unique_subcategories_by_norm = taxonomy_lookup(taxonomy)
    category_key = normalized_label(result.get("category"))
    category = categories_by_norm.get(category_key)
    subcategory_key = normalized_label(result.get("subcategory"))
    subcategory = subcategories_by_category_norm.get(category_key, {}).get(subcategory_key)
    if not subcategory and subcategory_key in unique_subcategories_by_norm:
        category, subcategory = unique_subcategories_by_norm[subcategory_key]
    if not subcategory:
        return None
    try:
        confidence = float(result.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0
    reason = normalize_text(result.get("reason") or result.get("reasoning") or "")
    if len(reason) > 180:
        reason = reason[:177].rstrip() + "..."
    return {
        "category": category,
        "subcategory": subcategory,
        "confidence": round(max(0.0, min(confidence, 1.0)), 4),
        "reasoning": reason,
        "model_name": OLLAMA_MODEL,
        "model_version": MODEL_VERSION,
    }


def product_text(product):
    payload = build_product_prompt_payload(product)
    fields = [
        payload.get("brand"),
        payload.get("authoritative_name"),
        payload.get("display_name"),
        payload.get("retailer"),
        payload.get("variation"),
        " ".join(payload.get("tags") or []),
        " ".join(payload.get("source_categories") or []),
        " ".join(payload.get("sources") or []),
        payload.get("url"),
    ]
    return normalize_text(" ".join(str(field or "") for field in fields)).lower()


def pair_key(category, subcategory):
    return f"{category}||{subcategory}"


def pair_from_key(value):
    category, subcategory = str(value).split("||", 1)
    return category, subcategory


def valid_taxonomy_pairs(taxonomy):
    return {
        pair_key(category.get("name"), subcategory.get("name"))
        for category in taxonomy.get("categories") or []
        for subcategory in category.get("subcategories") or []
    }


def taxonomy_gold_labels_path(base_dir):
    return os.path.join(base_dir, GOLD_LABELS_FILE)


def taxonomy_silver_labels_path(base_dir):
    return os.path.join(base_dir, SILVER_LABELS_FILE)


def taxonomy_clip_audit_path(base_dir):
    return os.path.join(base_dir, CLIP_AUDIT_FILE)


def normalized_identity_values(product):
    values = []
    for field in ("asin", "url", "raw_name", "name", "id"):
        value = product.get(field)
        if value:
            values.append(normalize_text(str(value)).casefold())
    return values


def product_identity_keys(product):
    keys = []
    for field in ("asin", "url", "raw_name", "name"):
        value = product.get(field)
        if value:
            keys.append(f"{field}:{normalize_text(str(value)).casefold()}")
    return keys


def normalize_gold_label(row, taxonomy):
    category = row.get("category") or row.get("reviewed_category")
    subcategory = row.get("subcategory") or row.get("reviewed_subcategory")
    normalized = normalize_model_result(
        {
            "category": category,
            "subcategory": subcategory,
            "confidence": row.get("confidence") or 1.0,
            "reasoning": row.get("notes") or row.get("review_notes") or "Manual gold taxonomy label.",
        },
        taxonomy,
    )
    if not normalized:
        return None
    normalized["confidence"] = 1.0
    normalized["reasoning"] = normalized.get("reasoning") or "Manual gold taxonomy label."
    normalized["model_name"] = "manual-gold-label"
    normalized["model_version"] = MODEL_VERSION
    return normalized


def normalize_silver_label(row, taxonomy):
    category = row.get("category") or row.get("reviewed_category")
    subcategory = row.get("subcategory") or row.get("reviewed_subcategory")
    confidence = float(row.get("confidence") or 0.82)
    confidence = max(0.55, min(confidence, 0.92))
    normalized = normalize_model_result(
        {
            "category": category,
            "subcategory": subcategory,
            "confidence": confidence,
            "reasoning": row.get("reason") or row.get("notes") or "OpenAI silver taxonomy backfill label.",
        },
        taxonomy,
    )
    if not normalized:
        return None
    normalized["confidence"] = confidence
    normalized["reasoning"] = normalized.get("reasoning") or "OpenAI silver taxonomy backfill label."
    normalized["model_name"] = "openai-silver-label"
    normalized["model_version"] = MODEL_VERSION
    normalized["label_source"] = "silver"
    return normalized


def load_gold_labels(base_dir, taxonomy):
    path = taxonomy_gold_labels_path(base_dir)
    payload = load_json_file(path, {"labels": []})
    rows = payload.get("labels") if isinstance(payload, dict) else payload
    rows = rows or []

    by_key = {}
    normalized_rows = []
    label_counts = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        result = normalize_gold_label(row, taxonomy)
        if not result:
            continue
        indexed = False
        for key in product_identity_keys(row):
            by_key[key] = result
            indexed = True
        if indexed:
            normalized_rows.append(row)
            label_counts[pair_key(result["category"], result["subcategory"])] += 1

    digest_payload = [
        {
            "asin": row.get("asin"),
            "url": row.get("url"),
            "name": row.get("name"),
            "raw_name": row.get("raw_name"),
            "category": row.get("category") or row.get("reviewed_category"),
            "subcategory": row.get("subcategory") or row.get("reviewed_subcategory"),
        }
        for row in normalized_rows
    ]
    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    return by_key, label_counts, digest


def load_silver_labels(base_dir, taxonomy):
    path = taxonomy_silver_labels_path(base_dir)
    payload = load_json_file(path, {"labels": []})
    rows = payload.get("labels") if isinstance(payload, dict) else payload
    rows = rows or []

    by_key = {}
    normalized_rows = []
    label_counts = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        result = normalize_silver_label(row, taxonomy)
        if not result:
            continue
        indexed = False
        for key in product_identity_keys(row):
            by_key[key] = result
            indexed = True
        if indexed:
            normalized_rows.append(row)
            label_counts[pair_key(result["category"], result["subcategory"])] += 1

    digest_payload = [
        {
            "asin": row.get("asin"),
            "url": row.get("url"),
            "name": row.get("name"),
            "raw_name": row.get("raw_name"),
            "category": row.get("category") or row.get("reviewed_category"),
            "subcategory": row.get("subcategory") or row.get("reviewed_subcategory"),
            "confidence": row.get("confidence"),
        }
        for row in normalized_rows
    ]
    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    return by_key, label_counts, digest


def gold_label_for_product(product, gold_index):
    for key in product_identity_keys(product):
        result = gold_index.get(key)
        if result:
            return dict(result)
    return None


def silver_label_for_product(product, silver_index):
    for key in product_identity_keys(product):
        result = silver_index.get(key)
        if result:
            return dict(result)
    return None


def load_clip_audit_labels(base_dir, taxonomy):
    path = taxonomy_clip_audit_path(base_dir)
    payload = load_json_file(path, {"results": []})
    rows = payload.get("results") if isinstance(payload, dict) else []
    rows = rows or []

    by_value = {}
    digest_payload = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clip = row.get("clip_locked_pair") or {}
        score = float(clip.get("score") or 0)
        source = clip.get("source") or "unknown"
        normalized = normalize_model_result(
            {
                "category": clip.get("category"),
                "subcategory": clip.get("subcategory"),
                "confidence": score,
                "reasoning": (
                    f"Locked CLIP suggestion ({source}, score {score:.4f}); "
                    "used only as a provisional fallback rescue."
                ),
            },
            taxonomy,
        )
        if not normalized:
            continue
        normalized["model_name"] = "local-clip-audit"
        normalized["model_version"] = MODEL_VERSION
        normalized["clip_source"] = source
        normalized["clip_score"] = score

        audit_product = row.get("product") or {}
        indexed = False
        for value in normalized_identity_values(audit_product):
            by_value[value] = normalized
            indexed = True
        if indexed:
            digest_payload.append(
                {
                    "id": audit_product.get("id"),
                    "name": audit_product.get("name"),
                    "raw_name": audit_product.get("raw_name"),
                    "category": normalized["category"],
                    "subcategory": normalized["subcategory"],
                    "source": source,
                    "score": round(score, 4),
                }
            )

    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    return by_value, digest


def clip_audit_label_for_product(product, clip_index):
    for value in normalized_identity_values(product):
        result = clip_index.get(value)
        if result:
            return dict(result)
    return None


def text_has_any(text, terms):
    return any(term in text for term in terms)


PACKAGED_PRODUCE_BLOCKERS = [
    "air bite",
    "bites",
    "breakfast cereal",
    "capsule",
    "capsules",
    "cereal",
    "chips",
    "coffee",
    "coffee blend",
    "conditioner",
    "conditoner",
    "cocktail",
    "crisps",
    "crispy",
    "curl",
    "drink",
    "extract",
    "fruit jerky",
    "gummies",
    "immunity blend",
    "immunity shot",
    "juice",
    "k-cup",
    "latte",
    "leave-in",
    "lotion",
    "medium roast",
    "mosquito",
    "pasta sauce",
    "pouch",
    "powder",
    "prebiotic",
    "probiotic",
    "protein",
    "repellent",
    "sauce",
    "sausage",
    "smoothie",
    "snack",
    "soda",
    "skin care",
    "superfood",
    "supplement",
    "tablet",
    "tea",
    "turkey tail",
    "fl oz",
    " fz",
    "yerba",
    "yerba mate",
]


FRUIT_TERMS = [
    "apple",
    "avocado",
    "avocados",
    "banana",
    "berries",
    "blackberries",
    "blueberries",
    "cantaloupe",
    "citrus",
    "dragon fruit",
    "grape",
    "grapes",
    "lemon",
    "lemons",
    "lime",
    "limes",
    "mango",
    "melon",
    "orange",
    "papaya",
    "pear",
    "pineapple",
    "plum",
    "strawberries",
]


SALAD_GREEN_TERMS = ["arugula", "lettuce", "salad greens", "spinach"]
MUSHROOM_TERMS = ["mushroom", "mushrooms"]
VEGETABLE_TERMS = [
    "artichoke",
    "artichokes",
    "asparagus",
    "beet",
    "beets",
    "broccoli",
    "brussels sprouts",
    "cabbage",
    "carrot",
    "carrots",
    "cauliflower",
    "celery",
    "corn",
    "cucumber",
    "cucumbers",
    "eggplant",
    "garlic",
    "ginger",
    "green beans",
    "onion",
    "onions",
    "pepper",
    "peppers",
    "potato",
    "potatoes",
    "radish",
    "radishes",
    "squash",
    "sweet potato",
    "tomato",
    "tomatoes",
    "zucchini",
]
HERB_TERMS = ["basil", "cilantro", "dill", "mint", "parsley", "rosemary", "sage", "thyme"]


def text_has_word(text, terms):
    return any(re.search(rf"\b{re.escape(term)}\b", text) for term in terms)


def text_has_phrase_or_word(text, phrases=(), words=()):
    return text_has_any(text, phrases) or text_has_word(text, words)


def is_packaged_or_processed_for_produce(text):
    return text_has_any(text, PACKAGED_PRODUCE_BLOCKERS)


def is_fresh_produce_text(text):
    if is_packaged_or_processed_for_produce(text):
        return False
    return (
        text_has_word(text, FRUIT_TERMS)
        or text_has_word(text, SALAD_GREEN_TERMS)
        or text_has_word(text, MUSHROOM_TERMS)
        or text_has_word(text, VEGETABLE_TERMS)
        or text_has_word(text, HERB_TERMS)
    )


def source_categories_text(product):
    return " ".join(str(value) for value in (product.get("source_categories") or []) if value).lower()


def has_source_fresh_produce(product):
    return "fresh produce" in source_categories_text(product)


def fresh_produce_source_classification(product, taxonomy):
    if not has_source_fresh_produce(product):
        return None
    text = product_text(product)
    if text_has_any(text, ["chunk", "chunks", "cut fruit", "cut vegetable", "fruit cup", "value pack"]):
        return local_result(taxonomy, "Produce", "Cut Fruit & Veg", reason="Whole Foods source labeled this card Fresh Produce.")
    if text_has_word(text, HERB_TERMS):
        return local_result(taxonomy, "Produce", "Fresh Herbs", reason="Whole Foods source labeled this card Fresh Produce.")
    if text_has_word(text, SALAD_GREEN_TERMS):
        return local_result(taxonomy, "Produce", "Salad Greens", reason="Whole Foods source labeled this card Fresh Produce.")
    if text_has_word(text, MUSHROOM_TERMS):
        return local_result(taxonomy, "Produce", "Mushrooms", reason="Whole Foods source labeled this card Fresh Produce.")
    if text_has_word(text, VEGETABLE_TERMS):
        return local_result(taxonomy, "Produce", "Vegetables", reason="Whole Foods source labeled this card Fresh Produce.")
    if text_has_word(text, FRUIT_TERMS):
        return local_result(taxonomy, "Produce", "Fruits", reason="Whole Foods source labeled this card Fresh Produce.")
    return local_result(
        taxonomy,
        "Produce",
        "Fruits",
        confidence=0.9,
        reason="Whole Foods source labeled this card Fresh Produce; subcategory defaulted to fruit.",
    )


def source_backed_classification(product, taxonomy):
    return fresh_produce_source_classification(product, taxonomy)


def local_result(taxonomy, category, subcategory, confidence=0.99, reason="High-confidence product text match."):
    if pair_key(category, subcategory) not in valid_taxonomy_pairs(taxonomy):
        return None
    return {
        "category": category,
        "subcategory": subcategory,
        "confidence": confidence,
        "reasoning": reason,
        "model_name": "local-taxonomy-classifier",
        "model_version": MODEL_VERSION,
    }


def packaged_form_classification(product, taxonomy):
    text = product_text(product)

    if text_has_any(text, ["cleanser", "face wash", "moisturizer", "face clay", "mud mask", "eye patches", "eye patch", "hydro gel", "serum", "aloe vera gel"]):
        return local_result(taxonomy, "Beauty & Personal Care", "Skin Care", reason="Skin-care treatment wording matched.")
    if text_has_any(text, ["anti-itch", "arthritis cream", "pain relieving", "healing balm", "body balm"]):
        return local_result(taxonomy, "Beauty & Personal Care", "Body Care", reason="Body-care treatment wording matched.")
    if text_has_phrase_or_word(text, phrases=["whole milk kefir", "low fat kefir"], words=["kefir"]):
        return local_result(taxonomy, "Dairy & Eggs", "Yogurt", reason="Kefir dairy wording matched.")
    if text_has_any(text, ["green tea single serve"]) or ("green tea" in text and not text_has_any(text, ["ready to drink", "iced tea"])):
        return local_result(taxonomy, "Beverages", "Tea", reason="Tea wording matched.")
    if text_has_any(text, ["electrolyte tabs", "electrolyte tab", "electrolyte tablets", "hydration tabs", "orange sunrise"]):
        return local_result(taxonomy, "Beverages", "Drink Mixes", reason="Electrolyte tablet wording matched.")
    if text_has_any(text, ["frozen pizza"]) or text_has_word(text, ["pizza"]):
        return local_result(taxonomy, "Frozen", "Frozen Pizza", reason="Pizza product wording matched.")
    if text_has_any(text, ["superfruit packs", "superfruit pack", "acai packs", "acai pack"]):
        return local_result(taxonomy, "Frozen", "Frozen Fruit", reason="Frozen acai pack wording matched.")
    if text_has_any(text, ["proper fries"]) or ("strong roots" in text and "fries" in text):
        return local_result(taxonomy, "Frozen", "Frozen Vegetables", reason="Frozen fries wording matched.")
    if text_has_any(text, ["breakfast cereal", "protein cereal", "catalina crunch", "granola", "oatmeal"]):
        return local_result(taxonomy, "Pantry", "Cereal & Breakfast", reason="Packaged breakfast cereal wording matched.")
    if text_has_any(text, ["kosher salt", "sea salt", "pink salt"]) and not text_has_any(text, ["chocolate", "bar", "snack", "chips"]):
        return local_result(taxonomy, "Pantry", "Spices & Seasonings", reason="Salt seasoning wording matched.")
    if text_has_any(text, ["mandarin"]) and text_has_any(text, ["oz", "ounce", "ounces"]):
        return local_result(taxonomy, "Pantry", "Canned Fruit", reason="Shelf-stable mandarin fruit wording matched.")
    if text_has_any(text, ["avocado oil", "coconut oil"]) and "essential oil" not in text:
        return local_result(taxonomy, "Pantry", "Oils & Vinegars", reason="Cooking-oil wording matched.")
    if text_has_any(text, ["farro"]):
        return local_result(taxonomy, "Pantry", "Rice & Grains", reason="Farro grain wording matched.")
    if text_has_any(text, ["baby food", "toddler", "kids snack", "smoothie pouch", "pouch"]):
        return local_result(taxonomy, "Baby", "Baby Food", reason="Baby or pouch food wording matched.")
    if text_has_any(text, ["mushroom supplement", "turkey tail", "lion's mane", "reishi", "chaga", "om mushroom", "mushroom superfood"]):
        return local_result(taxonomy, "Supplements & Wellness", "Mushroom Supplements", reason="Mushroom supplement wording matched.")
    if "coffee" in text and text_has_any(text, ["coffee blend", "ground", "grounds", "roast", "whole bean", "mushroom coffee"]):
        return local_result(taxonomy, "Beverages", "Coffee Beans & Grounds", reason="Coffee bean or ground coffee wording matched.")
    if text_has_any(text, ["body lotion", "daily lotion", "hand lotion", "lotion", "body cream"]):
        return local_result(taxonomy, "Beauty & Personal Care", "Body Care", reason="Body-care lotion wording matched.")
    if text_has_any(text, ["curl cream", "curl maker", "defining gel", "detangler", "conditioning spray", "conditoner", "repair hydrate", "leave-in", "co-wash", "deep treatment"]) or (
        text_has_word(text, ["curl", "curls"]) and text_has_any(text, ["cream", "gel", "spray", "conditioner", "conditoner", "treatment", "hair"])
    ):
        return local_result(taxonomy, "Beauty & Personal Care", "Hair Care", reason="Hair curl-care wording matched.")
    if text_has_any(text, ["mosquito repellent", "tick bug repellent", "bug repellent", "insect repellent"]):
        return local_result(taxonomy, "Household", "Insect Repellent", reason="Insect-repellent wording matched.")
    if text_has_any(text, ["food scrap bag", "food scrap bags", "compostable bag", "compostable bags"]):
        return local_result(taxonomy, "Household", "Trash Bags", reason="Compostable bag wording matched.")
    if text_has_any(text, ["incense stick", "incense sticks", "incense"]) and not text_has_any(text, ["spray", "oil"]):
        return local_result(taxonomy, "Household", "Air Fresheners", reason="Incense wording matched.")
    if text_has_any(text, ["bath tissue", "toilet paper", "paper towel", "paper towels"]):
        return local_result(taxonomy, "Household", "Paper Products", reason="Household paper-product wording matched.")
    if text_has_any(text, ["household gloves", "kitchen gloves", "cleaning gloves"]):
        return local_result(taxonomy, "Household", "Cleaning Supplies", reason="Household glove wording matched.")
    if text_has_any(text, ["multi-surface wipes", "cleaning wipes", "surface wipes"]):
        return local_result(taxonomy, "Household", "Cleaning Supplies", reason="Cleaning-wipe wording matched.")
    if text_has_any(text, ["dish liquid", "dish soap", "dishwashing soap", "dishwasher detergent", "dishwasher pods", "dishwasher tabs"]):
        return local_result(taxonomy, "Household", "Dishwashing", reason="Dishwashing wording matched.")
    if text_has_any(text, ["steamer basket", "zester", "can and bottle opener", "bottle opener"]):
        return local_result(taxonomy, "Household", "Kitchen Supplies", reason="Kitchen tool wording matched.")
    if text_has_any(text, ["room spray", "toilet spray", "air freshener"]):
        return local_result(taxonomy, "Household", "Air Fresheners", reason="Home fragrance spray wording matched.")
    if text_has_word(text, ["candle", "candles"]):
        return local_result(taxonomy, "Household", "Home Essentials", reason="Candle home-good wording matched.")
    if text_has_any(text, ["crispy air bites", "air bites"]) or ("strong roots" in text and "bites" in text):
        return local_result(taxonomy, "Frozen", "Frozen Appetizers", reason="Frozen crispy bite wording matched.")
    if text_has_any(text, ["chips", "crisps", "cracker", "crackers", "pretzel", "popcorn"]):
        return local_result(taxonomy, "Snacks", "Chips", reason="Packaged snack wording matched.")
    if text_has_any(text, ["magnesium glycinate", "night minerals"]) or (
        text_has_word(text, ["magnesium"]) and text_has_any(text, ["supplement", "drink stick", "nighttime drink", "calming nighttime"])
    ):
        return local_result(taxonomy, "Supplements & Wellness", "Minerals", reason="Mineral supplement wording matched.")
    if text_has_any(text, ["wellness shot", "immunity boost shot", "immunity shot", "gut well shot", "turmeric boost"]):
        return local_result(taxonomy, "Supplements & Wellness", "Wellness Shots", reason="Wellness-shot wording matched.")
    if text_has_any(text, ["nasal spray", "propolis nasal", "postbiotic", "probiotic", "sinus support", "immune system support", "immunity support", "black elderberry"]):
        return local_result(taxonomy, "Supplements & Wellness", "Immune Support", reason="Wellness support wording matched.")
    if text_has_any(text, ["maca extract", "wormwood extract", "echinacea", "ginkgo", "passionflower extract", "clove extract", "rhodiola extract", "kava extract", "turmeric root extract", "holy basil extract", "lavender extract", "propolis extract", "liquid extract"]):
        return local_result(taxonomy, "Supplements & Wellness", "Herbal Supplements", reason="Herbal extract wording matched.")
    if text_has_any(text, ["protein shot", "mental performance shot", "brain food", "koia elite"]):
        return local_result(taxonomy, "Supplements & Wellness", "Protein & Collagen", reason="Functional protein or performance shot wording matched.")
    if text_has_any(text, ["plant based patties", "plant-based patties", "impossible plant"]):
        return local_result(taxonomy, "Meat & Seafood", "Meat Alternatives", reason="Plant-based meat alternative wording matched.")
    if text_has_any(text, ["supergreens blend", "greens blend"]):
        return local_result(taxonomy, "Supplements & Wellness", "Herbal Supplements", reason="Greens supplement wording matched.")
    if text_has_any(text, ["smoothie", "immunity blend", "juice"]):
        return local_result(taxonomy, "Beverages", "Functional Drinks", reason="Drink or smoothie wording matched.")
    if text_has_any(text, ["powder", "capsule", "capsules", "tablet", "supplement", "superfood"]):
        return local_result(taxonomy, "Supplements & Wellness", "Herbal Supplements", confidence=0.95, reason="Supplement form wording matched.")
    return None


def guard_impossible_classification(product, result, taxonomy):
    if not result:
        return result
    fresh_produce = fresh_produce_source_classification(product, taxonomy)
    if fresh_produce and result.get("category") != "Produce":
        return fresh_produce
    if result.get("category") == "Produce" and not is_fresh_produce_text(product_text(product)):
        if fresh_produce:
            return fresh_produce
        replacement = packaged_form_classification(product, taxonomy)
        if replacement:
            replacement = dict(replacement)
            replacement["reasoning"] = (
                replacement.get("reasoning", "")
                + " Fresh Produce was blocked because packaged/processed product-form wording was present."
            ).strip()
            return replacement
        return local_result(
            taxonomy,
            "Pantry",
            "Meal Kits & Sides",
            confidence=0.35,
            reason="Fresh Produce was blocked because the product text looks packaged or processed.",
        )
    return result


def deterministic_classification(product, taxonomy):
    text = product_text(product)
    valid_pairs = valid_taxonomy_pairs(taxonomy)

    def result(category, subcategory, confidence=0.99, reason="High-confidence product text match."):
        if pair_key(category, subcategory) not in valid_pairs:
            return None
        return local_result(taxonomy, category, subcategory, confidence=confidence, reason=reason)

    fresh_produce = fresh_produce_source_classification(product, taxonomy)
    if fresh_produce:
        return fresh_produce

    # Non-food and wellness first so "gummies", "water", or "oil" do not steal supplements/care items.
    if text_has_any(text, ["shampoo", "conditioner", "hair mask", "scalp", "hair care", "curl cream", "curl maker", "detangler", "conditioning spray", "leave-in", "co-wash", "deep treatment"]) or (
        text_has_word(text, ["curl", "curls"]) and text_has_any(text, ["cream", "gel", "spray", "conditioner", "treatment", "hair"])
    ):
        return result("Beauty & Personal Care", "Hair Care", reason="Hair-care wording matched.")
    if text_has_any(text, ["lip gloss", "glasting color gloss", "lip balm", "lip care"]):
        return result("Beauty & Personal Care", "Lip Care", reason="Lip-care wording matched.")
    if text_has_any(text, ["sunscreen", "sunblock", "spf "]):
        return result("Beauty & Personal Care", "Sun Care", reason="Sun-care wording matched.")
    if text_has_any(text, ["hand soap", "body wash", "shower gel"]):
        return result("Beauty & Personal Care", "Soap & Hand Wash", reason="Soap or wash wording matched.")
    if text_has_any(text, ["serum", "moisturizer", "face cream", "skin cream", "cica cream", "eye cream", "ai cream", "physiogel", "facial", "skin care", "acne"]):
        return result("Beauty & Personal Care", "Skin Care", reason="Skin-care wording matched.")
    if text_has_any(text, ["cleanser", "face wash", "face clay", "mud mask", "eye patches", "eye patch", "hydro gel", "aloe vera gel"]):
        return result("Beauty & Personal Care", "Skin Care", reason="Skin-care treatment wording matched.")
    if text_has_any(text, ["body lotion", "daily lotion", "hand lotion", "lotion", "body cream"]):
        return result("Beauty & Personal Care", "Body Care", reason="Body-care lotion wording matched.")
    if text_has_any(text, ["anti-itch", "arthritis cream", "pain relieving", "healing balm", "body balm"]):
        return result("Beauty & Personal Care", "Body Care", reason="Body-care treatment wording matched.")
    if text_has_any(text, ["toothpaste", "mouthwash", "oral care", "toothbrush"]):
        return result("Beauty & Personal Care", "Oral Care", reason="Oral-care wording matched.")
    if text_has_any(text, ["deodorant"]):
        return result("Beauty & Personal Care", "Deodorant", reason="Deodorant wording matched.")

    if text_has_any(text, ["aluminum foil", "parchment", "plastic wrap", "wax paper", "sandwich bag", "resealable bag", "storage bag"]):
        return result("Household", "Foil, Wrap & Bags", reason="Food-wrap or bag wording matched.")
    if text_has_any(text, ["coffee filter", "filter basket"]):
        return result("Household", "Kitchen Supplies", reason="Kitchen supply wording matched.")
    if text_has_any(text, ["bug spray", "mosquito repellent", "tick bug repellent", "bug repellent", "insect repellent"]):
        return result("Household", "Insect Repellent", reason="Insect-repellent wording matched.")
    if text_has_any(text, ["household gloves", "kitchen gloves", "cleaning gloves"]):
        return result("Household", "Cleaning Supplies", reason="Household glove wording matched.")
    if text_has_any(text, ["steamer basket", "zester", "can and bottle opener", "bottle opener", "chef's knive", "chef's knife", "paring knife", "chopping mats"]):
        return result("Household", "Kitchen Supplies", reason="Kitchen tool wording matched.")
    if text_has_any(text, ["room spray", "toilet spray", "air freshener"]):
        return result("Household", "Air Fresheners", reason="Home fragrance spray wording matched.")
    if text_has_any(text, ["incense stick", "incense sticks", "incense"]) and not text_has_any(text, ["spray", "oil"]):
        return result("Household", "Air Fresheners", reason="Incense wording matched.")
    if text_has_word(text, ["candle", "candles"]):
        return result("Household", "Home Essentials", reason="Candle home-good wording matched.")
    if text_has_any(text, ["trash bag", "garbage bag"]):
        return result("Household", "Trash Bags", reason="Trash-bag wording matched.")
    if text_has_any(text, ["bath tissue", "toilet paper", "paper towel", "paper towels"]):
        return result("Household", "Paper Products", reason="Household paper-product wording matched.")
    if text_has_any(text, ["laundry detergent", "fabric softener"]):
        return result("Household", "Laundry", reason="Laundry wording matched.")
    if text_has_any(text, ["dish liquid", "dish soap", "dishwasher", "dishwashing"]):
        return result("Household", "Dishwashing", reason="Dishwashing wording matched.")
    if text_has_any(text, ["toilet cleaner", "cleaner refill", "cleaning tablet", "all purpose cleaner", "surface cleaner", "multi-surface wipes", "cleaning wipes", "surface wipes"]):
        return result("Household", "Cleaning Supplies", reason="Cleaning-supply wording matched.")

    form_result = packaged_form_classification(product, taxonomy)
    if form_result:
        return form_result

    if text_has_phrase_or_word(text, phrases=["whole milk kefir", "low fat kefir"], words=["kefir"]):
        return result("Dairy & Eggs", "Yogurt", reason="Kefir dairy wording matched.")
    if text_has_any(text, ["wellness shot", "immunity boost shot", "immunity shot", "gut well shot", "turmeric boost"]):
        return result("Supplements & Wellness", "Wellness Shots", reason="Wellness-shot wording matched.")
    if text_has_any(text, ["postbiotic", "probiotic", "sinus support", "immune system support", "immunity support", "black elderberry"]):
        return result("Supplements & Wellness", "Immune Support", reason="Wellness support wording matched.")
    if text_has_any(text, ["maca extract", "wormwood extract", "echinacea", "rhodiola extract", "kava extract", "turmeric root extract", "holy basil extract", "lavender extract", "propolis extract", "cat`s claw extract", "cat's claw extract", "liquid extract"]):
        return result("Supplements & Wellness", "Herbal Supplements", reason="Herbal extract wording matched.")
    if text_has_any(text, ["collagen", "protein powder", "protein peptides"]):
        return result("Supplements & Wellness", "Protein & Collagen", reason="Protein or collagen supplement wording matched.")
    if text_has_any(text, ["fish oil", "omega 3", "omega-3", " dha", " epa "]):
        return result("Supplements & Wellness", "Omega & Fish Oil", reason="Omega or fish-oil supplement wording matched.")
    if text_has_any(text, ["magnesium", "calcium", "zinc ", "omega 3", "omega-3", "mineral"]):
        return result("Supplements & Wellness", "Minerals", reason="Mineral supplement wording matched.")
    if text_has_any(text, ["multivitamin", "multi vitamin"]):
        return result("Supplements & Wellness", "Multivitamins", reason="Multivitamin wording matched.")
    if text_has_any(text, ["one daily", "women over 55", "men over 40"]):
        return result("Supplements & Wellness", "Multivitamins", reason="Daily multivitamin wording matched.")
    if text_has_any(text, ["vitamin d3", "vegan d3"]):
        return result("Supplements & Wellness", "Vitamins", reason="Vitamin D wording matched.")
    if text_has_any(text, ["vitamin", "elderberry", "immune support"]):
        return result("Supplements & Wellness", "Vitamins", reason="Vitamin or immune supplement wording matched.")
    if text_has_any(text, ["probiotic", "digestive", "cleanse", "constipation"]):
        return result("Supplements & Wellness", "Digestive Support", reason="Digestive wellness wording matched.")
    if text_has_any(text, ["melatonin", "sleep", "valerian", "bedtime"]):
        return result("Supplements & Wellness", "Sleep Support", reason="Sleep-support wording matched.")
    if text_has_any(text, ["ashwagandha", "stress support", "calm ", "calming"]):
        return result("Supplements & Wellness", "Stress Support", reason="Stress-support wording matched.")
    if text_has_any(text, ["essential oil", "aromatherapy", "diffuser oil"]):
        return result("Supplements & Wellness", "Essential Oils", reason="Essential-oil wording matched.")
    if text_has_any(text, ["capsule", "capsules", "softgel", "softgels", "vegetable capsules", "supplement"]):
        return result("Supplements & Wellness", "Herbal Supplements", confidence=0.95, reason="Supplement capsule wording matched.")
    if text_has_any(text, ["electrolyte tablet", "electrolyte tabs", "hydration tablet", "drink tablet", "hydration drink mix", "liquid iv", "sport hydration", "nuun hydration", "nuun sport", "orange sunrise"]):
        return result("Beverages", "Drink Mixes", reason="Drink-tablet wording matched.")

    alcohol_phrases = [
        "hard seltzer",
        "hard lemonade",
        "simply spiked",
        "busch light",
        "bota box",
        "pinot grigio",
        "sauvignon blanc",
        "red blend",
        "white wine",
        "wine bottle",
        "vodka mule",
        "mai tai",
        "cocktail syrup",
    ]
    alcohol_words = [
        "beer",
        "lager",
        "ipa",
        "ale",
        "stout",
        "porter",
        "wine",
        "abv",
        "lagunitas",
        "kirin",
        "modelo",
        "sake",
        "junmai",
        "vodka",
        "tequila",
        "whiskey",
        "bourbon",
        "rum",
        "gin",
        "brut",
        "cava",
        "merlot",
        "barolo",
        "moscato",
        "cocktail",
        "margarita",
        "proof",
    ]
    if text_has_phrase_or_word(text, phrases=alcohol_phrases, words=alcohol_words):
        if text_has_any(text, ["non-alcoholic", "non alcoholic", "hoppy refresher", "hop water", "athletic brewing"]):
            return result("Alcohol", "Non-Alcoholic Beer & Wine", reason="Non-alcoholic beer or hop beverage wording matched.")
        if text_has_phrase_or_word(text, phrases=["bota box", "pinot grigio", "sauvignon blanc", "red blend", "white wine", "wine bottle"], words=["wine", "sake", "junmai", "brut", "cava", "merlot", "barolo", "moscato"]):
            return result("Alcohol", "Wine", reason="Wine wording matched.")
        if text_has_any(text, ["hard seltzer"]):
            return result("Alcohol", "Hard Seltzer", reason="Hard-seltzer wording matched.")
        if text_has_phrase_or_word(text, phrases=["hard lemonade", "simply spiked", "vodka mule", "mai tai"], words=["vodka", "tequila", "whiskey", "bourbon", "rum", "gin", "cocktail", "margarita", "proof"]):
            return result("Alcohol", "Cocktails & Mixers", reason="Canned cocktail wording matched.")
        return result("Alcohol", "Beer", reason="Beer wording matched.")

    if text_has_any(text, ["cold brew concentrate", "coffee concentrate"]):
        return result("Beverages", "Coffee Concentrates", reason="Coffee concentrate wording matched.")
    if text_has_any(text, ["green tea single serve"]) or ("green tea" in text and not text_has_any(text, ["ready to drink", "iced tea"])):
        return result("Beverages", "Tea", reason="Tea wording matched.")
    if text_has_any(text, ["k-cup", "k cup", "coffee pod", "nespresso", "single serve coffee"]):
        return result("Beverages", "Coffee Pods & K-Cups", reason="Coffee pod wording matched.")
    if text_has_any(text, ["whole bean coffee", "ground coffee", "coffee grounds"]) or (
        "coffee" in text and text_has_any(text, ["coffee blend", "ground", "grounds", "roast", "whole bean", "mushroom coffee"])
    ):
        return result("Beverages", "Coffee Beans & Grounds", reason="Coffee bean or ground coffee wording matched.")
    if text_has_any(text, ["iced coffee", "cold brew coffee", "latte", "black coffee can", "ready to drink coffee"]):
        return result("Beverages", "Ready-to-Drink Coffee", reason="Ready-to-drink coffee wording matched.")
    if text_has_any(text, ["sparkling water", "seltzer"]):
        return result("Beverages", "Sparkling Water", reason="Sparkling-water wording matched.")
    if text_has_any(text, ["prebiotic soda", "soda"]):
        return result("Beverages", "Soda", reason="Soda wording matched.")
    if text_has_any(text, ["coconut water", "water coconut"]):
        return result("Beverages", "Coconut Water", reason="Coconut-water wording matched.")
    if text_has_any(text, ["apple juice", "orange juice", "aloe vera juice", "juice drink"]):
        return result("Beverages", "Juice", reason="Juice wording matched.")
    if text_has_any(text, ["kombucha"]):
        return result("Beverages", "Kombucha", reason="Kombucha wording matched.")
    if text_has_any(text, ["sports drink", "bodyarmor", "gatorade"]):
        return result("Beverages", "Sports Drinks", reason="Sports-drink wording matched.")
    if text_has_any(text, ["energy drink"]):
        return result("Beverages", "Energy Drinks", reason="Energy-drink wording matched.")
    if text_has_any(text, ["water", "alkaline water"]):
        return result("Beverages", "Water", reason="Water wording matched.")

    if text_has_any(text, ["tofu", "tempeh", "seitan"]):
        return result("Prepared Foods", "Tofu & Plant-Based Proteins", reason="Tofu or plant-protein wording matched.")
    if text_has_any(text, ["kimchi", "banchan"]):
        return result("Prepared Foods", "Kimchi & Banchan", reason="Kimchi or banchan wording matched.")
    if text_has_any(text, ["kimbap", "sushi"]):
        return result("Prepared Foods", "Sushi", reason="Sushi or kimbap wording matched.")
    if text_has_any(text, ["dumpling", "gyoza", "mandu"]):
        return result("Prepared Foods", "Dumplings & Quick Meals", reason="Dumpling wording matched.")

    if text_has_any(text, ["frozen pizza"]) or text_has_word(text, ["pizza"]):
        return result("Frozen", "Frozen Pizza", reason="Frozen pizza wording matched.")
    if text_has_any(text, ["superfruit packs", "superfruit pack", "acai packs", "acai pack"]):
        return result("Frozen", "Frozen Fruit", reason="Frozen acai pack wording matched.")
    if text_has_any(text, ["ice cream", "frozen dessert", "frozen novelt"]):
        return result("Frozen", "Ice Cream", reason="Frozen dessert wording matched.")
    if text_has_any(text, ["frozen fruit", "dragon fruit cubes"]):
        return result("Frozen", "Frozen Fruit", reason="Frozen fruit wording matched.")
    if text_has_any(text, ["proper fries"]) or ("strong roots" in text and "fries" in text):
        return result("Frozen", "Frozen Vegetables", reason="Frozen fries wording matched.")
    if text_has_any(text, ["frozen vegetable"]):
        return result("Frozen", "Frozen Vegetables", reason="Frozen vegetable wording matched.")
    if "frozen" in text:
        return result("Frozen", "Frozen Meals", confidence=0.94, reason="Frozen product wording matched.")

    if text_has_any(text, ["marinara", "pasta sauce"]):
        return result("Pantry", "Pasta Sauces", reason="Pasta-sauce wording matched.")
    if text_has_any(text, ["vanilla extract"]):
        return result("Pantry", "Baking Supplies", reason="Baking extract wording matched.")
    if text_has_any(text, ["tomato sauce", "tomatoes", "tomato product"]):
        return result("Pantry", "Tomatoes & Tomato Products", reason="Tomato pantry wording matched.")
    if text_has_any(text, ["sesame oil", "olive oil", "coconut oil", "vinegar"]):
        return result("Pantry", "Oils & Vinegars", reason="Oil or vinegar wording matched.")
    if text_has_any(text, ["avocado oil"]) and "essential oil" not in text:
        return result("Pantry", "Oils & Vinegars", reason="Cooking-oil wording matched.")
    if text_has_any(text, ["cooking oil spray", "oil spray", "high heat cooking oil"]):
        return result("Pantry", "Oils & Vinegars", reason="Cooking-oil wording matched.")
    if text_has_any(text, ["oyster sauce", "soy sauce", "fish sauce", "sukiyaki sauce", "marinade"]):
        return result("Pantry", "Marinades & Cooking Sauces", reason="Cooking-sauce wording matched.")
    if text_has_any(text, ["hummus"]):
        return result("Prepared Foods", "Salads & Sides", reason="Hummus dip wording matched.")
    if text_has_any(text, ["vinaigrette dressing"]):
        return result("Pantry", "Salad Dressings", reason="Salad dressing wording matched.")
    if text_has_any(text, ["labneh dip"]):
        return result("Prepared Foods", "Salads & Sides", reason="Refrigerated dip wording matched.")
    if text_has_any(text, ["parmesan sauce", "cracked pepper and parmesan", "sauz |"]):
        return result("Pantry", "Pasta Sauces", reason="Pasta-sauce wording matched.")
    if text_has_any(text, ["curry sauce", "curry hot", "curry mild", "curry mix"]):
        return result("International", "Asian Pantry", reason="Curry pantry wording matched.")
    if text_has_any(text, ["hot sauce", "bbq sauce", "sriracha"]):
        return result("Pantry", "Hot Sauce & BBQ Sauce", reason="Hot-sauce wording matched.")
    if text_has_any(text, ["mayo", "mayonnaise", "mustard"]):
        return result("Pantry", "Mayo & Mustard", reason="Mayo or mustard wording matched.")
    if text_has_any(text, ["ramen", "udon", "soba", "noodle"]):
        return result("International", "Asian Noodles & Dumplings", reason="Asian noodle wording matched.")
    if text_has_any(text, ["ravioli", "tortellini"]):
        return result("Prepared Foods", "Refrigerated Pasta & Noodles", reason="Refrigerated filled-pasta wording matched.")
    if text_has_any(text, ["rice", "grain"]):
        return result("Pantry", "Rice & Grains", reason="Rice or grain wording matched.")
    if text_has_any(text, ["chickpeas in glass jar", "chickpeas", "black beans"]):
        return result("Pantry", "Beans & Legumes", reason="Bean or legume wording matched.")
    if text_has_any(text, ["mandarin"]) and text_has_any(text, ["oz", "ounce", "ounces"]):
        return result("Pantry", "Canned Fruit", reason="Shelf-stable mandarin fruit wording matched.")
    if text_has_any(text, ["bamboo shoots"]):
        return result("Pantry", "Canned Vegetables", reason="Canned vegetable wording matched.")
    if text_has_any(text, ["farro"]):
        return result("Pantry", "Rice & Grains", reason="Farro grain wording matched.")
    if text_has_any(text, ["pasta", "mafald", "caserecce", "spaghetti"]):
        return result("Pantry", "Pasta", reason="Pasta wording matched.")
    if text_has_any(text, ["flour", "baking mix", "pancake mix"]):
        return result("Pantry", "Baking Supplies", reason="Baking supply wording matched.")
    if text_has_any(text, ["vanilla extract", "almond extract"]):
        return result("Pantry", "Baking Supplies", reason="Baking extract wording matched.")
    if text_has_any(text, ["honey", "jam", "bee pollen"]):
        return result("Pantry", "Jams & Honey", reason="Honey or jam wording matched.")

    if text_has_any(text, ["chips", "potato stick", "tortilla chip"]):
        return result("Snacks", "Chips", reason="Chip snack wording matched.")
    if text_has_any(text, ["cracker", "crackers"]):
        return result("Snacks", "Crackers", reason="Cracker snack wording matched.")
    if text_has_any(text, ["bunny grahams", "graham snacks", "toasties"]):
        return result("Snacks", "Cookies", reason="Sweet baked snack wording matched.")
    if text_has_any(text, ["jackfruit chews"]):
        return result("Snacks", "Fruit Snacks", reason="Fruit chew wording matched.")
    if text_has_any(text, ["cookie", "wafer", "wafel", "biscuit"]):
        return result("Snacks", "Cookies", reason="Cookie or wafer snack wording matched.")
    if text_has_any(text, ["candy", "chocolate", "gummy", "gummies", "jelly"]):
        return result("Snacks", "Candy & Chocolate", reason="Candy wording matched.")
    if text_has_any(text, ["protein bar", "snack bar"]) or (text_has_word(text, ["bars"]) and text_has_any(text, ["kind healthy snacks", "energy bars", "almond flour bars"])):
        return result("Snacks", "Snack Bars", reason="Snack-bar wording matched.")
    if text_has_any(text, ["popcorn"]):
        return result("Snacks", "Popcorn", reason="Popcorn wording matched.")

    if is_fresh_produce_text(text) and text_has_word(text, HERB_TERMS):
        return result("Produce", "Fresh Herbs", reason="Fresh herb wording matched.")
    if is_fresh_produce_text(text) and text_has_word(text, SALAD_GREEN_TERMS):
        return result("Produce", "Salad Greens", reason="Salad green wording matched.")
    if is_fresh_produce_text(text) and text_has_word(text, MUSHROOM_TERMS):
        return result("Produce", "Mushrooms", reason="Fresh mushroom wording matched.")
    if is_fresh_produce_text(text) and text_has_word(text, VEGETABLE_TERMS):
        return result("Produce", "Vegetables", reason="Fresh vegetable wording matched.")
    if is_fresh_produce_text(text) and text_has_word(text, FRUIT_TERMS):
        return result("Produce", "Fruits", reason="Fresh fruit wording matched.")

    if text_has_any(text, ["milk", "oatmilk", "almond milk", "soy milk"]):
        return result("Dairy & Eggs", "Milk", reason="Milk wording matched.")
    if text_has_any(text, ["creamer", "half and half"]):
        return result("Dairy & Eggs", "Cream & Creamers", reason="Creamer wording matched.")
    if text_has_any(text, ["yogurt", "fage", "w/hny total"]):
        return result("Dairy & Eggs", "Yogurt", reason="Yogurt wording matched.")
    if text_has_any(text, ["cheese", "feta", "burrata", "ossau iraty", "mozzarella", "cheddar", "brie"]):
        return result("Dairy & Eggs", "Cheese", reason="Cheese wording matched.")
    if text_has_any(text, ["egg", "eggs"]):
        return result("Dairy & Eggs", "Eggs", reason="Egg wording matched.")
    if text_has_any(text, ["butter"]):
        return result("Dairy & Eggs", "Butter & Margarine", reason="Butter wording matched.")

    if text_has_any(text, ["bagel", "bread", "english muffin"]):
        return result("Bakery", "Bread", reason="Bread or bagel wording matched.")
    if text_has_any(text, ["beignet", "beignets"]):
        return result("Bakery", "Pastries", reason="Pastry wording matched.")
    if text_has_any(text, ["cake", "cupcake"]):
        return result("Bakery", "Cakes & Cupcakes", reason="Cake wording matched.")
    if text_has_any(text, ["pastry", "croissant"]):
        return result("Bakery", "Pastries", reason="Pastry wording matched.")

    if text_has_any(text, ["beef", "wagyu"]):
        return result("Meat & Seafood", "Beef", reason="Beef wording matched.")
    if text_has_any(text, ["chicken"]):
        return result("Meat & Seafood", "Chicken", reason="Chicken wording matched.")
    if text_has_any(text, ["pork", "ham"]):
        return result("Meat & Seafood", "Pork", reason="Pork wording matched.")
    if text_has_any(text, ["lamb"]):
        return result("Meat & Seafood", "Lamb", reason="Lamb wording matched.")
    if text_has_any(text, ["bacon"]):
        return result("Meat & Seafood", "Bacon", reason="Bacon wording matched.")
    if text_has_any(text, ["salami", "prosciutto", "pepperoni", "sliced meat", "charcuterie"]):
        return result("Meat & Seafood", "Deli Meats", reason="Deli-meat wording matched.")
    if text_has_any(text, ["bratwurst", "sausage", "landjaeger", "meat sticks"]):
        return result("Meat & Seafood", "Sausage", reason="Sausage or meat-stick wording matched.")
    if text_has_any(text, ["shrimp"]):
        return result("Meat & Seafood", "Shrimp", reason="Shrimp wording matched.")
    if text_has_any(text, ["smoked atlantic salmon", "cold smoked atlantic salmon"]):
        return result("Meat & Seafood", "Smoked Seafood", reason="Smoked seafood wording matched.")
    if text_has_any(text, ["caviar"]):
        return result("Meat & Seafood", "Seafood", reason="Caviar seafood wording matched.")
    if text_has_any(text, ["scallop", "octopus", "squid", "salmon", "tuna", "seafood", "fish"]):
        return result("Meat & Seafood", "Seafood", reason="Seafood wording matched.")

    if text_has_any(text, ["baby food", "toddler", "diaper", "wipes"]):
        return result("Baby", "Baby Food", reason="Baby product wording matched.")
    if text_has_any(text, ["stasher", "food storage bowl"]):
        return result("Household", "Food Storage", reason="Food-storage wording matched.")

    return None



def batch_response_items(result):
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        return []
    for key in ("classifications", "products", "items", "results"):
        value = result.get(key)
        if isinstance(value, list):
            return value
    return []


def parse_index_batch_response(raw_result, raw_content, *, expected_count, item_count, index_key):
    items = [item for item in batch_response_items(raw_result) if isinstance(item, dict)]
    parsed_by_id = {}
    parsed_in_order = []
    for item in items:
        try:
            selected_index = int(item.get(index_key))
        except (TypeError, ValueError):
            selected_index = 0
        if not (1 <= selected_index <= item_count):
            parsed_in_order.append(None)
            continue
        try:
            confidence = float(item.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0
        reason = normalize_text(item.get("reason") or item.get("reasoning") or "")
        if len(reason) > 160:
            reason = reason[:157].rstrip() + "..."
        parsed = {
            "index": selected_index,
            "confidence": round(max(0.0, min(confidence, 1.0)), 4),
            "reason": reason,
        }
        parsed_in_order.append(parsed)
        try:
            local_id = int(str(item.get("id")).strip())
        except (TypeError, ValueError):
            local_id = 0
        if 1 <= local_id <= expected_count:
            parsed_by_id[local_id] = parsed

    if len(parsed_by_id) == expected_count:
        return [parsed_by_id[index] for index in range(1, expected_count + 1)]
    if len(parsed_in_order) == expected_count and all(parsed_in_order):
        return parsed_in_order
    raise RuntimeError(
        f"Batch classifier missed or returned invalid {index_key} values. Raw response: {raw_content[:1000]}"
    )


def chat_index_batch(*, system, prompt, expected_count, item_count, index_key):
    raw_result, raw_content = chat_json(
        system=system,
        prompt=prompt,
        timeout=OLLAMA_CHAT_TIMEOUT,
        retries=1,
        include_raw=True,
    )
    return parse_index_batch_response(
        raw_result,
        raw_content,
        expected_count=expected_count,
        item_count=item_count,
        index_key=index_key,
    )


def classify_product_batch(products, taxonomy, *, batch_start_index=1):
    if not ollama_available():
        raise RuntimeError("Ollama is not available for product classification.")

    products_payload = [
        build_compact_product_payload(product, offset + 1)
        for offset, product in enumerate(products)
    ]
    categories = taxonomy.get("categories") or []
    category_system = (
        "You classify grocery products into a fixed taxonomy for a shopping app. "
        "Return JSON only. Choose one exact category_index from the numbered category list for every product."
    )
    category_options = [
        format_category_option(index, category)
        for index, category in enumerate(categories, start=1)
    ]
    category_prompt = (
        "Choose the best top-level category for every product.\n"
        "Return JSON with shape "
        "{\"classifications\":[{\"id\":\"1\",\"category_index\":1,\"confidence\":0.0,\"reason\":\"short reason\"}]}.\n"
        "Rules:\n"
        f"{classification_rules_text()}\n"
        "- The reason must be one short sentence, 20 words or fewer.\n"
        "- If the product is non-food, use Household, Beauty & Personal Care, Baby, or Supplements & Wellness as appropriate.\n"
        "- Do not reuse a previous product's answer; classify each product independently.\n"
        "- category_index must be an integer from the numbered category list.\n\n"
        f"Numbered categories:\n{chr(10).join(category_options)}\n\n"
        f"Products:\n{json.dumps(products_payload, ensure_ascii=False)}"
    )
    category_results = chat_index_batch(
        system=category_system,
        prompt=category_prompt,
        expected_count=len(products),
        item_count=len(categories),
        index_key="category_index",
    )

    results = [None] * len(products)
    grouped = defaultdict(list)
    for offset, category_result in enumerate(category_results):
        category_index = category_result["index"]
        grouped[category_index].append((offset, category_result))

    subcategory_system = (
        "You classify grocery products into subcategories inside one already-chosen category. "
        "Return JSON only. Choose one exact subcategory_index from the numbered subcategory list for every product."
    )
    for category_index, grouped_items in grouped.items():
        category = categories[category_index - 1]
        subcategories = category.get("subcategories") or []
        subcategory_options = [
            format_subcategory_option(index, category["name"], subcategory)
            for index, subcategory in enumerate(subcategories, start=1)
        ]
        grouped_payload = [
            build_compact_product_payload(products[offset], local_index)
            for local_index, (offset, _) in enumerate(grouped_items, start=1)
        ]
        subcategory_prompt = (
            f"Top-level category already selected: {category['name']}.\n"
            f"Category guidance: {CATEGORY_GUIDANCE.get(category['name'], 'Use the category name literally.')}\n"
            "Choose the best subcategory for every product.\n"
            "Return JSON with shape "
            "{\"classifications\":[{\"id\":\"1\",\"subcategory_index\":1,\"confidence\":0.0,\"reason\":\"short reason\"}]}.\n"
            "Rules:\n"
            f"{classification_rules_text()}\n"
            "- The reason must be one short sentence, 20 words or fewer.\n"
            "- Do not reuse a previous product's answer; classify each product independently.\n"
            "- subcategory_index must be an integer from the numbered subcategory list.\n\n"
            f"Numbered subcategories:\n{chr(10).join(subcategory_options)}\n\n"
            f"Products:\n{json.dumps(grouped_payload, ensure_ascii=False)}"
        )
        subcategory_results = chat_index_batch(
            system=subcategory_system,
            prompt=subcategory_prompt,
            expected_count=len(grouped_items),
            item_count=len(subcategories),
            index_key="subcategory_index",
        )
        for (offset, category_result), subcategory_result in zip(grouped_items, subcategory_results):
            subcategory = subcategories[subcategory_result["index"] - 1]
            confidence = round((category_result["confidence"] + subcategory_result["confidence"]) / 2.0, 4)
            reason = normalize_text(
                f"Category: {category_result['reason']} Subcategory: {subcategory_result['reason']}"
            )
            if len(reason) > 220:
                reason = reason[:217].rstrip() + "..."
            results[offset] = {
                "category": category["name"],
                "subcategory": subcategory["name"],
                "confidence": confidence,
                "reasoning": reason,
                "model_name": OLLAMA_MODEL,
                "model_version": MODEL_VERSION,
            }

    if not all(results):
        missing = [str(batch_start_index + offset) for offset, result in enumerate(results) if not result]
        raise RuntimeError(f"Batch classifier did not produce results for product ids: {', '.join(missing)}")
    return results


def classify_product_batch_safely(products, taxonomy, *, batch_start_index=1):
    try:
        return classify_product_batch(products, taxonomy, batch_start_index=batch_start_index)
    except Exception:
        if len(products) <= 1:
            raise
        midpoint = len(products) // 2
        first = classify_product_batch_safely(
            products[:midpoint],
            taxonomy,
            batch_start_index=batch_start_index,
        )
        second = classify_product_batch_safely(
            products[midpoint:],
            taxonomy,
            batch_start_index=batch_start_index + midpoint,
        )
        return first + second


def hydrate_product_with_classification(product, result, fingerprint):
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
    updated["ai_taxonomy_version"] = result.get("taxonomy_version")
    updated["ai_fingerprint"] = fingerprint
    updated["ai_label_source"] = result.get("label_source") or "model"
    if result.get("clip_source"):
        updated["ai_clip_source"] = result.get("clip_source")
    if result.get("clip_score") is not None:
        updated["ai_clip_score"] = result.get("clip_score")
    apply_failed_classification_bucket(updated)
    return updated


def is_failed_classification_record(product):
    category = product.get("category") or product.get("ai_category")
    subcategory = product.get("subcategory") or product.get("ai_subcategory")
    return not category or not subcategory


def apply_failed_classification_bucket(product):
    if product.get("classification_status") == "failed":
        return product
    if not is_failed_classification_record(product):
        product["classification_status"] = product.get("classification_status") or "classified"
        return product

    original_category = product.get("category") or product.get("ai_category") or "Uncategorized"
    original_subcategory = product.get("subcategory") or product.get("ai_subcategory") or "Miscellaneous"
    product["classification_status"] = "failed"
    product["failed_from_category"] = original_category
    product["failed_from_subcategory"] = original_subcategory
    product["failed_reason"] = product.get("ai_reasoning") or "Missing or low-confidence taxonomy label."
    product["category"] = FAILED_CATEGORY
    product["subcategory"] = FAILED_SUBCATEGORY
    product["ai_category"] = FAILED_CATEGORY
    product["ai_subcategory"] = FAILED_SUBCATEGORY
    product["category_confidence"] = round(float(product.get("category_confidence") or 0), 4)
    product["ai_confidence"] = round(float(product.get("ai_confidence") or product.get("category_confidence") or 0), 4)
    return product


def existing_valid_classification(product, taxonomy):
    key = pair_key(product.get("category"), product.get("subcategory"))
    if key not in valid_taxonomy_pairs(taxonomy):
        return None
    return {
        "category": product.get("category"),
        "subcategory": product.get("subcategory"),
        "confidence": 0.72,
        "reasoning": "Learned from existing valid taxonomy label.",
        "model_name": "local-taxonomy-classifier",
        "model_version": MODEL_VERSION,
    }


def build_training_examples(products, taxonomy, gold_index=None, silver_index=None, gold_weight=8, silver_weight=3):
    texts = []
    labels = []
    seen = set()
    for product in products:
        text = product_text(product)
        if not text:
            continue
        label_result = gold_label_for_product(product, gold_index or {})
        weight = gold_weight if label_result else 1
        label_source = "gold" if label_result else "local"
        if not label_result:
            label_result = silver_label_for_product(product, silver_index or {})
            if label_result:
                weight = silver_weight
                label_source = "silver"
        if not label_result:
            label_result = source_backed_classification(product, taxonomy)
        if not label_result:
            continue
        label = pair_key(label_result["category"], label_result["subcategory"])
        key = (text, label, label_source)
        if key not in seen:
            seen.add(key)
            for _ in range(weight):
                texts.append(text)
                labels.append(label)
    return texts, labels


def train_text_classifier(products, taxonomy, gold_index=None, silver_index=None):
    texts, labels = build_training_examples(products, taxonomy, gold_index=gold_index, silver_index=silver_index)
    if len(set(labels)) < 2 or len(texts) < 8:
        return None, Counter(labels)
    model = Pipeline(
        [
            (
                "features",
                FeatureUnion(
                    [
                        (
                            "word",
                            TfidfVectorizer(
                                analyzer="word",
                                ngram_range=(1, 2),
                                min_df=1,
                                max_features=50000,
                                sublinear_tf=True,
                            ),
                        ),
                        (
                            "char",
                            TfidfVectorizer(
                                analyzer="char_wb",
                                ngram_range=(3, 5),
                                min_df=1,
                                max_features=80000,
                                sublinear_tf=True,
                            ),
                        ),
                    ]
                ),
            ),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    solver="liblinear",
                ),
            ),
        ]
    )
    model.fit(texts, labels)
    return model, Counter(labels)


def ml_classification_with_threshold(product, model, taxonomy, *, min_confidence, reason):
    if model is None:
        return None
    text = product_text(product)
    if not text:
        return None
    probabilities = model.predict_proba([text])[0]
    best_index = max(range(len(probabilities)), key=lambda index: probabilities[index])
    label = model.classes_[best_index]
    if label not in valid_taxonomy_pairs(taxonomy):
        return None
    category, subcategory = pair_from_key(label)
    confidence = round(float(probabilities[best_index]), 4)
    if confidence < min_confidence:
        return None
    return {
        "category": category,
        "subcategory": subcategory,
        "confidence": confidence,
        "reasoning": reason,
        "model_name": "local-sklearn",
        "model_version": MODEL_VERSION,
    }


def ml_classification(product, model, taxonomy):
    return ml_classification_with_threshold(
        product,
        model,
        taxonomy,
        min_confidence=0.2,
        reason="Local text classifier prediction.",
    )


def best_effort_ml_classification(product, model, taxonomy):
    return ml_classification_with_threshold(
        product,
        model,
        taxonomy,
        min_confidence=0.0,
        reason="Best-effort local text classifier rescue after deterministic and CLIP fallback.",
    )


def default_classification(product, taxonomy):
    packaged = packaged_form_classification(product, taxonomy)
    if packaged:
        packaged = dict(packaged)
        packaged["confidence"] = min(packaged["confidence"], 0.75)
        packaged["reasoning"] = "Local fallback selected a product-form taxonomy label."
        return packaged
    return {
        "category": "Pantry",
        "subcategory": "Meal Kits & Sides",
        "confidence": 0.25,
        "reasoning": "Local model produced a low-confidence general pantry label.",
        "model_name": "local-taxonomy-classifier",
        "model_version": MODEL_VERSION,
    }


def clip_classification(product, clip_index, taxonomy, *, min_score, confidence, label_source, reason_prefix):
    clip = clip_audit_label_for_product(product, clip_index)
    if not clip:
        return None

    score = float(clip.get("clip_score") or clip.get("confidence") or 0)
    source = clip.get("clip_source") or "unknown"
    if score < min_score:
        return None

    normalized = normalize_model_result(
        {
            "category": clip.get("category"),
            "subcategory": clip.get("subcategory"),
            "confidence": confidence,
            "reasoning": (
                f"{reason_prefix} "
                f"Locked pair source={source}, score={score:.4f}. "
                "Gold/manual labels still override this."
            ),
        },
        taxonomy,
    )
    if not normalized:
        return None
    normalized["model_name"] = "local-clip-audit"
    normalized["model_version"] = MODEL_VERSION
    normalized["label_source"] = label_source
    normalized["clip_source"] = source
    normalized["clip_score"] = round(score, 4)
    return normalized


def clip_primary_classification(product, clip_index, taxonomy):
    return clip_classification(
        product,
        clip_index,
        taxonomy,
        min_score=CLIP_PRIMARY_MIN_SCORE,
        confidence=0.78,
        label_source="clip-primary",
        reason_prefix="Primary locked CLIP taxonomy label.",
    )


def clip_rescue_classification(product, clip_index, taxonomy):
    return clip_classification(
        product,
        clip_index,
        taxonomy,
        min_score=CLIP_RESCUE_MIN_SCORE,
        confidence=0.62,
        label_source="clip-provisional",
        reason_prefix="Provisional CLIP rescue for an otherwise unresolved fallback item.",
    )


def choose_taxonomy_index(*, system, prompt, item_count, images=None):
    for attempt in range(3):
        current_prompt = prompt
        if attempt:
            current_prompt = (
                prompt
                + "\n\nYour previous answer did not use a valid pair_index from the numbered list. "
                  "Reply again using only one exact pair_index from the list."
            )
        use_images = images if attempt == 0 else None
        result = chat_json(
            system=system,
            prompt=current_prompt,
            images=use_images,
            timeout=OLLAMA_CHAT_TIMEOUT,
            retries=1,
        )
        try:
            pair_index = int(result.get("pair_index"))
        except (TypeError, ValueError):
            pair_index = 0
        if 1 <= pair_index <= item_count:
            confidence = float(result.get("confidence") or 0)
            reasoning = normalize_text(result.get("reasoning"))
            return pair_index, max(0.0, min(confidence, 1.0)), reasoning
    raise RuntimeError("Model did not return a valid pair_index from the provided taxonomy list.")


def classify_one_product(product, taxonomy):
    if not ollama_available():
        raise RuntimeError("Ollama is not available for product classification.")

    categories = taxonomy.get("categories") or []
    if not categories:
        raise RuntimeError("Cannot classify products because the discovered taxonomy has no categories.")

    product_payload = build_product_prompt_payload(product)
    image_b64 = fetch_image_as_base64(product.get("image"))

    category_list = [
        format_category_option(index, category)
        for index, category in enumerate(categories, start=1)
    ]
    category_system = (
        "You classify products for a grocery shopping app. "
        "Return JSON only with keys: pair_index, confidence, reasoning. "
        "Choose the best top-level category from the numbered list. "
        "Use authoritative_name first. If display_name is short or abbreviated, trust authoritative_name more. "
        "Do not choose a category merely because no perfect match appears; choose the category whose guidance best describes the product."
    )
    category_prompt = (
        "Choose the best top-level category for this product.\n"
        "You must choose exactly one pair_index from the numbered category list.\n"
        "General rules:\n"
        f"{classification_rules_text()}\n\n"
        f"Available categories:\n{chr(10).join(category_list)}\n\n"
        f"Product:\n{json.dumps(product_payload, ensure_ascii=False)}"
    )
    category_index, category_confidence, category_reasoning = choose_taxonomy_index(
        system=category_system,
        prompt=category_prompt,
        item_count=len(categories),
        images=[image_b64] if image_b64 else None,
    )
    chosen_category = categories[category_index - 1]

    subcategories = chosen_category.get("subcategories") or []
    if not subcategories:
        raise RuntimeError(f"Chosen category has no subcategories: {chosen_category.get('name')}")

    subcategory_list = [
        format_subcategory_option(index, chosen_category["name"], subcategory)
        for index, subcategory in enumerate(subcategories, start=1)
    ]
    subcategory_system = (
        "You classify products for a grocery shopping app. "
        "Return JSON only with keys: pair_index, confidence, reasoning. "
        "Choose the best subcategory from the numbered list inside the already-selected top-level category. "
        "Use authoritative_name first. If display_name is short or abbreviated, trust authoritative_name more. "
        "Use the subcategory descriptions when present."
    )
    subcategory_prompt = (
        f"The top-level category is already chosen: {chosen_category['name']}.\n"
        f"Category guidance: {CATEGORY_GUIDANCE.get(chosen_category['name'], 'Use the category name literally.')}\n"
        "Choose the best subcategory for this product.\n"
        "You must choose exactly one pair_index from the numbered subcategory list.\n"
        "General rules:\n"
        f"{classification_rules_text()}\n\n"
        f"Available subcategories:\n{chr(10).join(subcategory_list)}\n\n"
        f"Product:\n{json.dumps(product_payload, ensure_ascii=False)}"
    )
    subcategory_index, subcategory_confidence, subcategory_reasoning = choose_taxonomy_index(
        system=subcategory_system,
        prompt=subcategory_prompt,
        item_count=len(subcategories),
        images=[image_b64] if image_b64 else None,
    )
    chosen_subcategory = subcategories[subcategory_index - 1]

    confidence = round((category_confidence + subcategory_confidence) / 2.0, 4)
    reasoning = normalize_text(
        f"Category: {category_reasoning} Subcategory: {subcategory_reasoning}"
    )
    return {
        "category": chosen_category["name"],
        "subcategory": chosen_subcategory["name"],
        "confidence": confidence,
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
    if taxonomy and taxonomy.get("taxonomy_version") == FIXED_TAXONOMY_VERSION and not force_rediscover:
        return taxonomy

    taxonomy = discover_taxonomy(products)
    save_json_file(path, taxonomy)
    return taxonomy


def classify_products(base_dir, products, force_rediscover=False):
    taxonomy = ensure_taxonomy(base_dir, products, force_rediscover=force_rediscover)
    gold_index, gold_label_counts, gold_digest = load_gold_labels(base_dir, taxonomy)
    silver_index, silver_label_counts, silver_digest = load_silver_labels(base_dir, taxonomy)
    clip_index, clip_digest = load_clip_audit_labels(base_dir, taxonomy)
    cache_path = build_cache_artifact(base_dir)
    report_path = build_report_artifact(base_dir)
    expected_cache_metadata = {
        "taxonomy_version": taxonomy.get("taxonomy_version"),
        "prompt_version": PROMPT_VERSION,
        "model_name": "local-taxonomy-classifier",
        "model_version": MODEL_VERSION,
        "gold_labels_digest": gold_digest,
        "silver_labels_digest": silver_digest,
        "clip_audit_digest": clip_digest,
        "clip_primary_min_score": CLIP_PRIMARY_MIN_SCORE,
        "clip_rescue_min_score": CLIP_RESCUE_MIN_SCORE,
    }
    cache = load_json_file(cache_path, {**expected_cache_metadata, "items": {}})
    if any(cache.get(key) != value for key, value in expected_cache_metadata.items()):
        cache = {**expected_cache_metadata, "items": {}}

    print("[taxonomy] training local text classifier")
    model, label_counts = train_text_classifier(products, taxonomy, gold_index=gold_index, silver_index=silver_index)
    print(f"[taxonomy] local training labels: {sum(label_counts.values())} across {len(label_counts)} taxonomy pairs")
    print(f"[taxonomy] gold labels: {sum(gold_label_counts.values())} across {len(gold_label_counts)} taxonomy pairs")
    print(f"[taxonomy] silver labels: {sum(silver_label_counts.values())} across {len(silver_label_counts)} taxonomy pairs")
    print(f"[taxonomy] clip audit labels: {len(clip_index)} identity keys")

    changed = []
    updated_products = []
    total_count = len(products)
    for index, product in enumerate(products, start=1):
        product_name = product.get("name") or product.get("raw_name") or "(unnamed product)"
        fingerprint = product_fingerprint(product, taxonomy.get("taxonomy_version"))
        gold_result = gold_label_for_product(product, gold_index)
        if gold_result:
            result = gold_result
            result["label_source"] = "gold"
            result["taxonomy_version"] = taxonomy.get("taxonomy_version")
            cache.setdefault("items", {})[fingerprint] = result
            print(f"[taxonomy] {index}/{total_count} gold label: {product_name}")
        else:
            cached = (cache.get("items") or {}).get(fingerprint)
            if cached and not classification_is_bootstrap(cached):
                result = dict(cached)
                result["cache_hit"] = True
                print(f"[taxonomy] {index}/{total_count} cache hit: {product_name}")
            else:
                result = source_backed_classification(product, taxonomy)
                if result:
                    print(f"[taxonomy] {index}/{total_count} source-backed: {product_name}")
                else:
                    result = silver_label_for_product(product, silver_index)
                    if result:
                        result["label_source"] = "silver"
                        print(f"[taxonomy] {index}/{total_count} silver label: {product_name}")
                    else:
                        result = clip_primary_classification(product, clip_index, taxonomy)
                        if result:
                            print(f"[taxonomy] {index}/{total_count} clip primary: {product_name}")
                        else:
                            result = deterministic_classification(product, taxonomy)
                            if result:
                                print(f"[taxonomy] {index}/{total_count} deterministic: {product_name}")
                            else:
                                result = ml_classification(product, model, taxonomy)
                                if result:
                                    print(f"[taxonomy] {index}/{total_count} local ml: {product_name}")
                                else:
                                    result = clip_rescue_classification(product, clip_index, taxonomy)
                                    if result:
                                        print(f"[taxonomy] {index}/{total_count} clip provisional: {product_name}")
                                    else:
                                        result = best_effort_ml_classification(product, model, taxonomy)
                                        if result:
                                            print(f"[taxonomy] {index}/{total_count} best-effort ml: {product_name}")
                                        else:
                                            result = default_classification(product, taxonomy)
                                            print(f"[taxonomy] {index}/{total_count} local low-confidence: {product_name}")
                if result.get("label_source") != "gold":
                    result = guard_impossible_classification(product, result, taxonomy)
                result = dict(result)
                result["taxonomy_version"] = taxonomy.get("taxonomy_version")
                cache.setdefault("items", {})[fingerprint] = result
                changed.append(product.get("raw_name") or product.get("name"))
                save_json_file(cache_path, cache)
                save_classification_progress(
                    report_path,
                    taxonomy=taxonomy,
                    total_count=total_count,
                    changed=changed,
                    completed_count=index,
                    last_product=product_name,
                )
        if result.get("label_source") != "gold":
            result = guard_impossible_classification(product, result, taxonomy)
        updated_products.append(hydrate_product_with_classification(product, result, fingerprint))

    save_json_file(cache_path, cache)
    save_classification_progress(
        report_path,
        taxonomy=taxonomy,
        total_count=len(updated_products),
        changed=changed,
        completed_count=len(updated_products),
        last_product=updated_products[-1].get("name") if updated_products else None,
    )
    return updated_products, taxonomy
