#!/usr/bin/env python3
import argparse
import html
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_REPORTS_DIR = BASE_DIR / "reports"
FAILED_CATEGORY = "Other/Failed"
FAILED_SUBCATEGORY = "Needs Review"
LOW_CONFIDENCE_MAX = 0.35

SOURCE_FILES = {
    "Flyer": "flyer_products.json",
    "All Deals": "discovered_products.json",
    "Search Deals": "search_deals_products.json",
    "Target Deals": "target_deals_products.json",
    "H Mart Deals": "hmart_deals_products.json",
}

IMPORTANT_FIELDS = [
    "asin",
    "asins",
    "name",
    "raw_name",
    "brand",
    "source_brand",
    "brand_source",
    "retailer",
    "sources",
    "source_count",
    "category",
    "subcategory",
    "classification_status",
    "failed_from_category",
    "failed_from_subcategory",
    "category_confidence",
    "ai_category",
    "ai_subcategory",
    "ai_confidence",
    "ai_label_source",
    "ai_model_name",
    "ai_model_version",
    "ai_clip_score",
    "ai_clip_source",
    "current_price",
    "basis_price",
    "prime_price",
    "discount",
    "discount_percent",
    "unit_price",
    "image",
    "url",
    "retail_source_url",
    "source_categories",
    "tags",
    "available_store_ids",
    "store_offers",
]

STOPWORDS = {
    "and",
    "the",
    "with",
    "for",
    "organic",
    "natural",
    "whole",
    "foods",
    "market",
    "count",
    "ounce",
    "ounces",
    "pack",
    "fl",
    "oz",
    "ct",
    "ea",
    "lb",
    "gluten",
    "free",
    "vegan",
    "non",
    "gmo",
}


def load_json(path, default):
    path = Path(path)
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def normalize_text(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def compact_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def esc(value):
    return html.escape(str(value if value is not None else ""))


def pct(part, total):
    if not total:
        return "0.0%"
    return f"{(part / total) * 100:.1f}%"


def product_identity(product):
    asin = product.get("asin") or ""
    asins = product.get("asins") or []
    if asin:
        return f"asin:{asin}"
    if asins:
        return f"asin:{asins[0]}"
    url = product.get("url") or product.get("retail_source_url") or ""
    if url:
        return f"url:{compact_key(url)}"
    return f"name:{compact_key(product.get('retailer'))}:{compact_key(product.get('brand'))}:{compact_key(product.get('raw_name') or product.get('name'))}"


def product_clip_keys(product):
    keys = set()
    for value in [product.get("asin"), *(product.get("asins") or [])]:
        if value:
            keys.add(f"asin:{value}")
            keys.add(str(value))
    for value in [product.get("name"), product.get("raw_name")]:
        if value:
            keys.add(f"name:{normalize_text(value)}")
            keys.add(normalize_text(value))
    return keys


def build_clip_index(clip_report):
    index = {}
    for result in clip_report.get("results") or []:
        product = result.get("product") or {}
        for value in [product.get("id")]:
            if value:
                index[f"asin:{value}"] = result
                index[str(value)] = result
        for value in [product.get("name"), product.get("raw_name")]:
            if value:
                index[f"name:{normalize_text(value)}"] = result
                index[normalize_text(value)] = result
    return index


def clip_for_product(product, clip_index):
    for key in product_clip_keys(product):
        if key in clip_index:
            return clip_index[key]
    return None


def clip_score(result):
    if not result:
        return None
    locked = result.get("clip_locked_pair") or {}
    category = result.get("clip_category") or {}
    subcategory = result.get("clip_subcategory") or {}
    score = locked.get("score")
    if score is None:
        score = category.get("score")
    if score is None:
        score = subcategory.get("score")
    try:
        return round(float(score), 4)
    except (TypeError, ValueError):
        return None


def is_failed_product(product):
    if product.get("classification_status") == "failed":
        return True
    category = product.get("category") or product.get("ai_category")
    subcategory = product.get("subcategory") or product.get("ai_subcategory")
    confidence = float(product.get("ai_confidence") or product.get("category_confidence") or 0)
    reasoning = normalize_text(product.get("ai_reasoning"))
    return (
        (not category or not subcategory)
        or (
            category == "Pantry"
            and subcategory == "Meal Kits & Sides"
            and confidence <= LOW_CONFIDENCE_MAX
            and "low confidence general pantry label" in reasoning
        )
    )


def confidence_bucket(value):
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        value = 0
    if value >= 0.9:
        return "0.90-1.00"
    if value >= 0.75:
        return "0.75-0.89"
    if value >= 0.55:
        return "0.55-0.74"
    if value >= 0.35:
        return "0.35-0.54"
    return "0.00-0.34"


def sample_products(products, limit=12):
    return products[:limit]


def product_summary(product):
    return {
        field: product.get(field)
        for field in IMPORTANT_FIELDS
        if product.get(field) not in (None, "", [], {})
    }


def words_for_product(product):
    text = normalize_text(" ".join(str(product.get(field) or "") for field in ("brand", "name", "raw_name")))
    return [
        word
        for word in text.split()
        if len(word) >= 4 and word not in STOPWORDS and not word.isdigit()
    ]


def suspicious_category_reason(product):
    text = normalize_text(" ".join(str(product.get(field) or "") for field in ("name", "raw_name", "brand", "source_brand")))
    category = product.get("category")
    if category == FAILED_CATEGORY:
        return "Failed fallback bucket."
    if category == "Produce" and re.search(r"\b(gummy|gummies|soda|coffee|tea|shampoo|serum|capsule|tablet|supplement|extract|shot|water|juice|spray)\b", text):
        return "Produce label with packaged/drink/supplement/personal-care words."
    if category == "Pantry" and re.search(r"\b(shampoo|conditioner|serum|cream|spray|capsule|tablet|collagen|magnesium|probiotic|vitamin|extract|oil)\b", text):
        return "Pantry label with likely wellness or personal-care words."
    if category == "Beverages" and re.search(r"\b(shampoo|conditioner|serum|cream|spray|capsule|tablet|essential oil)\b", text):
        return "Beverage label with non-drink words."
    if category == "Dairy & Eggs" and re.search(r"\b(pizza|pasta|snack|bar|vegan cheese|dairy free)\b", text):
        return "Dairy label with possible prepared/frozen/snack/dairy-alternative ambiguity."
    if category == "Alcohol" and re.search(r"\b(hummus|snack|salami|seltzer water|water)\b", text):
        return "Alcohol label with likely non-alcohol product words."
    return None


def brand_quality_reason(product):
    brand = product.get("brand") or ""
    source_brand = product.get("source_brand") or ""
    if not brand:
        return "Missing brand."
    if brand.lower() in {"fresh produce", "produce", "unknown", "none", "select"}:
        return "Suspicious generic brand."
    if source_brand and brand and compact_key(source_brand) != compact_key(brand):
        if compact_key(source_brand) not in compact_key(brand) and compact_key(brand) not in compact_key(source_brand):
            return "Brand differs from source_brand."
    return None


def data_quality_reasons(product):
    reasons = []
    if not product.get("image"):
        reasons.append("Missing image")
    if not product.get("url"):
        reasons.append("Missing product URL")
    if not (product.get("prime_price") or product.get("current_price")):
        reasons.append("Missing sale/current price")
    if not product.get("discount"):
        reasons.append("Missing discount text")
    try:
        discount = float(product.get("discount_percent") or 0)
    except (TypeError, ValueError):
        discount = 0
    if discount >= 80:
        reasons.append("Very high discount percent")
    if discount == 0 and product.get("discount"):
        reasons.append("Discount text did not parse into percent")
    return reasons


def row_table(counter, title, limit=20):
    rows = counter.most_common(limit)
    if not rows:
        return f"<p class=\"muted\">No {esc(title.lower())} found.</p>"
    body = "".join(
        f"<tr><td>{esc(label)}</td><td>{count:,}</td></tr>"
        for label, count in rows
    )
    return f"<table><thead><tr><th>{esc(title)}</th><th>Count</th></tr></thead><tbody>{body}</tbody></table>"


def product_card(product, clip=None):
    image = product.get("image")
    image_html = f"<img src=\"{esc(image)}\" alt=\"\">" if image else "<div class=\"no-image\">No image</div>"
    url = product.get("url") or product.get("retail_source_url")
    title = esc(product.get("name") or product.get("raw_name") or "(unnamed)")
    title_html = f"<a href=\"{esc(url)}\" target=\"_blank\" rel=\"noreferrer\">{title}</a>" if url else title
    sources = ", ".join(product.get("sources") or [])
    clip_label = ""
    if clip:
        locked = clip.get("clip_locked_pair") or {}
        clip_label = f"<span class=\"pill\">CLIP {esc(locked.get('category'))} / {esc(locked.get('subcategory'))} · {clip_score(clip)}</span>"
    failed_label = ""
    if is_failed_product(product):
        failed_label = f"<span class=\"pill warn\">Failed from {esc(product.get('failed_from_category') or product.get('category'))} / {esc(product.get('failed_from_subcategory') or product.get('subcategory'))}</span>"
    details = "".join(
        f"<tr><td>{esc(field)}</td><td><code>{esc(json.dumps(product.get(field), ensure_ascii=False))}</code></td></tr>"
        for field in IMPORTANT_FIELDS
        if product.get(field) not in (None, "", [], {})
    )
    return f"""
    <article class="product-card">
      <div class="thumb">{image_html}</div>
      <div class="card-body">
        <h4>{title_html}</h4>
        <p class="muted">{esc(product.get('brand') or 'No brand')} · {esc(product.get('retailer') or 'Unknown retailer')} · {esc(sources)}</p>
        <p><span class="pill">{esc(product.get('category'))} / {esc(product.get('subcategory'))}</span>
        <span class="pill">conf {esc(product.get('ai_confidence') or product.get('category_confidence') or 0)}</span>
        <span class="pill">{esc(product.get('ai_label_source') or 'unknown')}</span>
        {failed_label}{clip_label}</p>
        <p class="price">{esc(product.get('prime_price') or product.get('current_price') or '')} <span>{esc(product.get('discount') or '')}</span></p>
        <details><summary>Show all product fields</summary><table>{details}</table></details>
      </div>
    </article>
    """


def build_store_label_map(combined_report):
    labels = {}
    for store in (combined_report or {}).get("whole_foods_stores") or []:
        store_id = str(store.get("store_id") or "").strip()
        store_name = store.get("store_name") or store_id
        if store_id:
            labels[store_id] = store_name
    return labels


def store_offer_signature(offer):
    return {
        "current_price": offer.get("current_price"),
        "basis_price": offer.get("basis_price"),
        "prime_price": offer.get("prime_price"),
        "discount": offer.get("discount"),
        "discount_percent": offer.get("discount_percent"),
        "unit_price": offer.get("unit_price"),
    }


def source_summary(values):
    return " + ".join(sorted(set(values or []))) if values else "Unknown"


def product_section(title, products, clip_index, empty="Nothing to show here.", limit=12):
    shown = sample_products(products, limit)
    if not shown:
        return f"<section><h2>{title}</h2><p class=\"muted\">{esc(empty)}</p></section>"
    cards = "\n".join(product_card(product, clip_for_product(product, clip_index)) for product in shown)
    return f"<section><h2>{title}</h2><div class=\"cards\">{cards}</div></section>"


def build_audit(products, clip_report, combined_report, taxonomy_report, source_counts):
    clip_index = build_clip_index(clip_report)
    total = len(products)
    failed = [product for product in products if is_failed_product(product)]
    by_category = Counter(product.get("category") or "Missing" for product in products)
    by_subcategory = Counter(f"{product.get('category') or 'Missing'} / {product.get('subcategory') or 'Missing'}" for product in products)
    by_retailer = Counter(product.get("retailer") or "Unknown" for product in products)
    by_label_source = Counter(product.get("ai_label_source") or "unknown" for product in products)
    by_model = Counter(product.get("ai_model_name") or "unknown" for product in products)
    by_confidence = Counter(confidence_bucket(product.get("ai_confidence") or product.get("category_confidence")) for product in products)
    by_sources = Counter(" + ".join(product.get("sources") or ["Unknown"]) for product in products)

    products_with_clip = []
    products_missing_clip = []
    clip_disagreements = []
    low_clip = []
    for product in products:
        clip = clip_for_product(product, clip_index)
        if clip:
            products_with_clip.append(product)
            score = clip_score(clip)
            if score is not None and score < 0.55:
                low_clip.append(product)
            locked = clip.get("clip_locked_pair") or {}
            if (
                score is not None
                and score >= 0.75
                and locked.get("category")
                and locked.get("category") != product.get("category")
            ):
                clip_disagreements.append(product)
        else:
            products_missing_clip.append(product)

    source_overlap = defaultdict(list)
    for product in products:
        source_overlap[tuple(product.get("sources") or ["Unknown"])].append(product)

    suspicious = []
    for product in products:
        reason = suspicious_category_reason(product)
        if reason:
            enriched = dict(product)
            enriched["audit_reason"] = reason
            suspicious.append(enriched)

    brand_issues = []
    for product in products:
        reason = brand_quality_reason(product)
        if reason:
            enriched = dict(product)
            enriched["audit_reason"] = reason
            brand_issues.append(enriched)

    data_issues = []
    for product in products:
        reasons = data_quality_reasons(product)
        if reasons:
            enriched = dict(product)
            enriched["audit_reason"] = "; ".join(reasons)
            data_issues.append(enriched)

    duplicate_groups = defaultdict(list)
    for product in products:
        name_key = compact_key(product.get("name") or product.get("raw_name"))
        if name_key:
            duplicate_groups[name_key].append(product)
    duplicateish = [
        group
        for group in duplicate_groups.values()
        if len(group) > 1 and (len({p.get("retailer") for p in group}) > 1 or len({p.get("brand") for p in group}) > 1)
    ]
    duplicateish.sort(key=len, reverse=True)

    store_labels = build_store_label_map(combined_report)
    whole_foods_products = [product for product in products if (product.get("retailer") or "") == "Whole Foods"]
    store_presence_counter = Counter()
    store_specific_products = []
    store_price_diff_products = []
    store_source_diff_products = []

    for product in whole_foods_products:
        offers = list(product.get("store_offers") or [])
        available_store_ids = [str(value) for value in (product.get("available_store_ids") or []) if value]

        if offers:
            offer_ids = [str(offer.get("store_id") or "") for offer in offers if offer.get("store_id")]
            if offer_ids:
                store_presence_counter.update(offer_ids)

        store_ids = sorted(set(available_store_ids or [str(offer.get("store_id") or "") for offer in offers if offer.get("store_id")]))
        if not store_ids:
            continue

        if len(store_ids) == 1:
            enriched = dict(product)
            only_store_id = store_ids[0]
            enriched["audit_reason"] = f"Only available in {store_labels.get(only_store_id, only_store_id)}."
            enriched["store_specificity"] = {
                "type": "single-store",
                "store_ids": store_ids,
                "store_sources": {
                    only_store_id: source_summary((offers[0].get("sources") if offers else product.get("sources")) or [])
                },
            }
            store_specific_products.append(enriched)
            continue

        signature_map = {}
        source_map = {}
        for offer in offers:
            store_id = str(offer.get("store_id") or "")
            if not store_id:
                continue
            signature_map[store_id] = store_offer_signature(offer)
            source_map[store_id] = tuple(sorted(set(offer.get("sources") or [])))

        if len({json.dumps(value, sort_keys=True) for value in signature_map.values() if value}) > 1:
            enriched = dict(product)
            enriched["audit_reason"] = "Store prices or deal fields differ across Whole Foods stores."
            enriched["store_specificity"] = {
                "type": "price-diff",
                "store_ids": store_ids,
                "store_signatures": signature_map,
                "store_sources": {store_id: source_summary(source_map.get(store_id) or []) for store_id in store_ids},
            }
            store_price_diff_products.append(enriched)

        if len(set(source_map.values())) > 1:
            enriched = dict(product)
            enriched["audit_reason"] = "Store source coverage differs across Whole Foods stores."
            enriched["store_specificity"] = {
                "type": "source-diff",
                "store_ids": store_ids,
                "store_signatures": signature_map,
                "store_sources": {store_id: source_summary(source_map.get(store_id) or []) for store_id in store_ids},
            }
            store_source_diff_products.append(enriched)

    failed_brand_clusters = Counter(product.get("brand") or "Missing brand" for product in failed)
    failed_word_clusters = Counter()
    for product in failed:
        failed_word_clusters.update(words_for_product(product))

    recommendations = [
        {
            "title": "Review the failed bucket first",
            "detail": f"{len(failed):,} products are in {FAILED_CATEGORY}. Gold-labeling the top brand/token clusters will reduce visible failures fastest.",
        },
        {
            "title": "Refresh CLIP coverage after big scrapes",
            "detail": f"{len(products_missing_clip):,} products have no matching CLIP audit row. Those cannot benefit from the vision fallback until `vision_category_audit.full.json` is regenerated.",
        },
        {
            "title": "Prefer source-backed rules over broad keyword guessing",
            "detail": "Use reliable source fields like Fresh Produce, retailer breadcrumbs, and product-detail metadata before adding keyword rules that can move drinks, supplements, or personal care into the wrong shelf.",
        },
        {
            "title": "Use gold labels as training data",
            "detail": "Pick representative products from repeated failed brands and product families. One good label can help a whole cluster through the local text model.",
        },
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_products": total,
            "failed_products": len(failed),
            "failed_percent": pct(len(failed), total),
            "clip_covered_products": len(products_with_clip),
            "clip_missing_products": len(products_missing_clip),
            "clip_missing_percent": pct(len(products_missing_clip), total),
            "suspicious_category_products": len(suspicious),
            "brand_issue_products": len(brand_issues),
            "data_issue_products": len(data_issues),
            "duplicateish_groups": len(duplicateish),
        },
        "source_file_counts": source_counts,
        "combined_report": combined_report,
        "taxonomy_report": taxonomy_report,
        "counts": {
            "categories": dict(by_category),
            "subcategories": dict(by_subcategory),
            "retailers": dict(by_retailer),
            "label_sources": dict(by_label_source),
            "models": dict(by_model),
            "confidence_buckets": dict(by_confidence),
            "source_overlaps": dict(by_sources),
            "failed_brand_clusters": dict(failed_brand_clusters),
            "failed_word_clusters": dict(failed_word_clusters),
            "whole_foods_store_presence": {
                store_labels.get(store_id, store_id): count
                for store_id, count in store_presence_counter.items()
            },
        },
        "findings": {
            "failed_products": [product_summary(product) for product in failed],
            "suspicious_categories": [product_summary(product) for product in suspicious],
            "clip_high_confidence_disagreements": [product_summary(product) for product in clip_disagreements],
            "clip_low_score_products": [product_summary(product) for product in low_clip],
            "clip_missing_products": [product_summary(product) for product in products_missing_clip],
            "brand_issues": [product_summary(product) for product in brand_issues],
            "data_issues": [product_summary(product) for product in data_issues],
            "duplicateish_groups": [
                [product_summary(product) for product in group]
                for group in duplicateish[:50]
            ],
            "whole_foods_store_specific_products": [product_summary(product) for product in store_specific_products],
            "whole_foods_store_price_differences": [product_summary(product) for product in store_price_diff_products],
            "whole_foods_store_source_differences": [product_summary(product) for product in store_source_diff_products],
            "source_overlap_examples": {
                " + ".join(sources): [product_summary(product) for product in sample_products(group, 20)]
                for sources, group in sorted(source_overlap.items(), key=lambda item: len(item[1]), reverse=True)
            },
        },
        "recommendations": recommendations,
        "_runtime": {
            "clip_index": clip_index,
            "failed": failed,
            "suspicious": suspicious,
            "brand_issues": brand_issues,
            "data_issues": data_issues,
            "clip_disagreements": clip_disagreements,
            "products_missing_clip": products_missing_clip,
            "low_clip": low_clip,
            "duplicateish": duplicateish,
            "store_specific_products": store_specific_products,
            "store_price_diff_products": store_price_diff_products,
            "store_source_diff_products": store_source_diff_products,
            "store_labels": store_labels,
        },
    }


def render_html(audit, products):
    runtime = audit["_runtime"]
    clip_index = runtime["clip_index"]
    summary = audit["summary"]
    counts = audit["counts"]
    store_labels = runtime["store_labels"]
    urgent = []
    urgent.extend(runtime["failed"][:4])
    urgent.extend(runtime["suspicious"][:4])
    urgent.extend(runtime["clip_disagreements"][:4])

    stat_cards = "".join(
        f"<div class=\"stat\"><strong>{esc(value)}</strong><span>{esc(label)}</span></div>"
        for label, value in [
            ("Products", f"{summary['total_products']:,}"),
            ("Failed", f"{summary['failed_products']:,} ({summary['failed_percent']})"),
            ("CLIP missing", f"{summary['clip_missing_products']:,} ({summary['clip_missing_percent']})"),
            ("Suspicious category", f"{summary['suspicious_category_products']:,}"),
            ("Brand issues", f"{summary['brand_issue_products']:,}"),
            ("Data issues", f"{summary['data_issue_products']:,}"),
        ]
    )
    recommendation_html = "".join(
        f"<li><strong>{esc(item['title'])}</strong><br>{esc(item['detail'])}</li>"
        for item in audit["recommendations"]
    )
    failed_cluster_tables = (
        row_table(Counter(counts["failed_brand_clusters"]), "Failed brands", limit=15)
        + row_table(Counter(counts["failed_word_clusters"]), "Failed product words", limit=20)
    )
    duplicate_sections = ""
    for index, group in enumerate(runtime["duplicateish"][:8], start=1):
        duplicate_sections += f"<h3>Duplicate-ish group {index} ({len(group)} products)</h3><div class=\"cards\">"
        duplicate_sections += "".join(product_card(product, clip_for_product(product, clip_index)) for product in group[:8])
        duplicate_sections += "</div>"

    source_overlap_cards = ""
    for source_label, count in Counter(counts["source_overlaps"]).most_common(8):
        related = [product for product in products if " + ".join(product.get("sources") or ["Unknown"]) == source_label]
        source_overlap_cards += f"<h3>{esc(source_label)} · {count:,}</h3><div class=\"cards\">"
        source_overlap_cards += "".join(product_card(product, clip_for_product(product, clip_index)) for product in related[:8])
        source_overlap_cards += "</div>"

    store_presence_table = row_table(Counter(counts.get("whole_foods_store_presence") or {}), "Whole Foods store presence", limit=20)

    def store_diff_cards(products_for_section, title_builder):
        section_html = ""
        for product in products_for_section[:18]:
            info = product.get("store_specificity") or {}
            labels = [store_labels.get(store_id, store_id) for store_id in info.get("store_ids") or []]
            store_sources = info.get("store_sources") or {}
            badges = " · ".join(
                f"{store_labels.get(store_id, store_id)}: {store_sources.get(store_id, 'Unknown')}"
                for store_id in info.get("store_ids") or []
            )
            section_html += (
                f"<div class=\"store-note\"><strong>{esc(title_builder(product, labels))}</strong>"
                f"<br><span class=\"muted\">{esc(badges)}</span></div>"
                + product_card(product, clip_for_product(product, clip_index))
            )
        return section_html

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Catalog Quality Audit</title>
  <style>
    :root {{ --ink:#173326; --muted:#66756c; --surface:#fffdf7; --line:#e7dfce; --brand:#2f7a45; --warn:#a6402c; --soft:#f7efe0; }}
    body {{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--ink); background:linear-gradient(135deg,#fbf6ea,#eef6ee); }}
    main {{ max-width:1240px; margin:0 auto; padding:32px 18px 64px; }}
    header {{ padding:28px; border:1px solid rgba(255,255,255,.8); border-radius:28px; background:rgba(255,253,247,.82); box-shadow:0 18px 70px rgba(31,54,42,.12); }}
    h1 {{ margin:0 0 8px; font-size:clamp(34px,5vw,64px); letter-spacing:-.05em; }}
    h2 {{ margin:42px 0 14px; font-size:28px; letter-spacing:-.03em; }}
    h3 {{ margin:22px 0 10px; }}
    .muted {{ color:var(--muted); }}
    .stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-top:20px; }}
    .stat {{ background:var(--surface); border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .stat strong {{ display:block; font-size:26px; }}
    .stat span {{ color:var(--muted); font-weight:700; }}
    section {{ margin-top:28px; }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(290px,1fr)); gap:14px; }}
    .product-card {{ display:grid; grid-template-columns:84px 1fr; gap:12px; background:var(--surface); border:1px solid var(--line); border-radius:20px; padding:12px; }}
    .thumb {{ width:84px; height:84px; display:grid; place-items:center; background:#fff; border-radius:14px; overflow:hidden; }}
    .thumb img {{ width:76px; height:76px; object-fit:contain; }}
    .no-image {{ font-size:11px; color:var(--muted); text-align:center; }}
    h4 {{ margin:0 0 6px; font-size:15px; line-height:1.25; }}
    a {{ color:var(--brand); text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    .pill {{ display:inline-block; margin:2px 4px 2px 0; padding:4px 7px; border-radius:999px; background:#edf5ed; color:#245c35; font-size:11px; font-weight:800; }}
    .pill.warn {{ background:#fff1d6; color:#8a5b00; }}
    .price {{ margin:6px 0; color:var(--warn); font-weight:900; }}
    .price span {{ color:#8f6a5a; font-size:12px; }}
    table {{ width:100%; border-collapse:collapse; background:var(--surface); border-radius:14px; overflow:hidden; }}
    th,td {{ padding:8px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
    code {{ white-space:pre-wrap; overflow-wrap:anywhere; font-size:11px; color:#435148; }}
    details {{ margin-top:8px; }}
    summary {{ cursor:pointer; color:var(--brand); font-weight:800; }}
    .two-col {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:18px; }}
    .callout {{ background:#173326; color:#fffdf7; border-radius:22px; padding:18px; }}
    .callout li {{ margin:0 0 12px; }}
    .store-note {{ margin:14px 0 6px; }}
  </style>
</head>
<body>
<main>
  <header>
    <h1>🧪 Catalog Quality Audit</h1>
    <p class="muted">Generated {esc(audit['generated_at'])}. This is the “show me the weird stuff” dashboard: failed taxonomy, CLIP behavior, source overlap, brand weirdness, and product evidence.</p>
    <div class="stats">{stat_cards}</div>
  </header>

  <section class="callout">
    <h2>🚑 How we get failed numbers down</h2>
    <ol>{recommendation_html}</ol>
  </section>

  {product_section("🔥 Most urgent examples", urgent, clip_index, "No urgent examples found.", limit=12)}

  <section>
    <h2>📊 Classification Health</h2>
    <div class="two-col">
      {row_table(Counter(counts['categories']), 'Category', limit=30)}
      {row_table(Counter(counts['label_sources']), 'Label source', limit=20)}
      {row_table(Counter(counts['models']), 'Model', limit=20)}
      {row_table(Counter(counts['confidence_buckets']), 'Confidence bucket', limit=10)}
    </div>
  </section>

  <section>
    <h2>🧯 Failed Bucket Clusters</h2>
    <p class="muted">These clusters are the fastest path to shrinking {FAILED_CATEGORY}: gold-label repeated brands/product words first.</p>
    <div class="two-col">{failed_cluster_tables}</div>
  </section>

  {product_section("🕳️ Products in Other/Failed", runtime['failed'], clip_index, "No failed products. Tiny parade.", limit=30)}
  {product_section("🧐 Suspicious Category Examples", runtime['suspicious'], clip_index, "No suspicious category examples found.", limit=24)}

  <section>
    <h2>🔁 Source Overlap</h2>
    <div class="two-col">
      {row_table(Counter(counts['source_overlaps']), 'Source overlap', limit=20)}
      {row_table(Counter(counts['retailers']), 'Retailer', limit=10)}
    </div>
    {source_overlap_cards}
  </section>

  <section>
    <h2>🏪 Whole Foods Store Specificity</h2>
    <p class="muted">This section shows whether Columbus Circle and Upper West Side actually differ, and if they do, which source created the difference: Flyer, All Deals, or Search Deals.</p>
    <div class="two-col">
      {store_presence_table}
      <table><thead><tr><th>Metric</th><th>Count</th></tr></thead><tbody>
        <tr><td>Single-store Whole Foods products</td><td>{len(runtime['store_specific_products']):,}</td></tr>
        <tr><td>Cross-store products with price/deal differences</td><td>{len(runtime['store_price_diff_products']):,}</td></tr>
        <tr><td>Cross-store products with source differences</td><td>{len(runtime['store_source_diff_products']):,}</td></tr>
      </tbody></table>
    </div>
    <h3>Only in one Whole Foods store</h3>
    <div class="cards">{store_diff_cards(runtime['store_specific_products'], lambda product, labels: f"{product.get('name') or 'Unknown'} · only in {', '.join(labels)}") or '<p class="muted">No single-store products found.</p>'}</div>
    <h3>Whole Foods products with different prices across stores</h3>
    <div class="cards">{store_diff_cards(runtime['store_price_diff_products'], lambda product, labels: f"{product.get('name') or 'Unknown'} · price differs across {', '.join(labels)}") or '<p class="muted">No cross-store price differences found.</p>'}</div>
    <h3>Whole Foods products with different source coverage across stores</h3>
    <div class="cards">{store_diff_cards(runtime['store_source_diff_products'], lambda product, labels: f"{product.get('name') or 'Unknown'} · source coverage differs across {', '.join(labels)}") or '<p class="muted">No cross-store source differences found.</p>'}</div>
  </section>

  {product_section("👁️ High-confidence CLIP Disagreements", runtime['clip_disagreements'], clip_index, "No high-confidence CLIP disagreements found.", limit=24)}
  {product_section("🌫️ Low-score CLIP Cases", runtime['low_clip'], clip_index, "No low-score CLIP cases found.", limit=18)}
  {product_section("🚫 Missing CLIP Coverage", runtime['products_missing_clip'], clip_index, "Every product had CLIP coverage.", limit=18)}

  {product_section("🏷️ Brand Weirdness", runtime['brand_issues'], clip_index, "No brand issues found.", limit=24)}
  {product_section("🧾 Deal/Data Quality Issues", runtime['data_issues'], clip_index, "No data quality issues found.", limit=24)}

  <section>
    <h2>🧬 Duplicate-ish Product Groups</h2>
    <p class="muted">Same normalized name, but conflicting retailer or brand. Some are okay; these are just worth a look.</p>
    {duplicate_sections or '<p class="muted">No duplicate-ish groups found.</p>'}
  </section>
</main>
</body>
</html>"""


def strip_runtime(audit):
    clean = dict(audit)
    clean.pop("_runtime", None)
    return clean


def main():
    parser = argparse.ArgumentParser(description="Build a detailed HTML catalog quality audit dashboard.")
    parser.add_argument("--products", default=str(BASE_DIR / "combined_products.json"))
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORTS_DIR))
    args = parser.parse_args()

    products = load_json(args.products, [])
    combined_report = load_json(BASE_DIR / "combined_report.json", {})
    taxonomy_report = load_json(BASE_DIR / "taxonomy_ai_report.json", {})
    clip_report = load_json(BASE_DIR / "vision_category_audit.full.json", {})
    source_counts = {
        label: len(load_json(BASE_DIR / filename, []))
        for label, filename in SOURCE_FILES.items()
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    audit = build_audit(products, clip_report, combined_report, taxonomy_report, source_counts)
    html_report = render_html(audit, products)

    html_path = output_dir / "catalog_quality_audit.html"
    json_path = output_dir / "catalog_quality_audit.json"
    queue_path = output_dir / "failed_products_review_queue.json"

    html_path.write_text(html_report, encoding="utf-8")
    write_json(json_path, strip_runtime(audit))
    write_json(queue_path, audit["findings"]["failed_products"])

    summary = audit["summary"]
    print(f"Wrote {html_path}")
    print(f"Wrote {json_path}")
    print(f"Wrote {queue_path}")
    print(
        "Summary: "
        f"{summary['total_products']:,} products, "
        f"{summary['failed_products']:,} failed ({summary['failed_percent']}), "
        f"{summary['clip_missing_products']:,} missing CLIP ({summary['clip_missing_percent']})."
    )


if __name__ == "__main__":
    main()
