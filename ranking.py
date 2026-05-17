import math
import re


FAILED_CATEGORY = "Other/Failed"
PRODUCE_EDGE_CASE_KEYWORDS = {
    "pierogi",
    "pesto",
    "soup",
    "broth",
    "snack",
    "fruit snack",
    "fruit snacks",
    "bar soap",
    "beard oil",
}
FORM_FACTOR_STOPWORDS = {
    "organic",
    "fresh",
    "market",
    "whole",
    "foods",
    "brand",
    "natural",
    "mini",
    "large",
    "small",
    "count",
    "ct",
    "oz",
    "lb",
    "ea",
    "pack",
    "bag",
    "box",
}


def normalize_text_key(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def effective_category(product):
    return (
        product.get("ai_category")
        or product.get("category")
        or FAILED_CATEGORY
    )


def effective_subcategory(product):
    return product.get("ai_subcategory") or product.get("subcategory") or ""


def parse_price_value(text):
    if not text:
        return math.inf
    match = re.search(r"\$([0-9]+(?:\.[0-9]+)?)", str(text))
    if not match:
        return math.inf
    return float(match.group(1))


def current_price_value(product):
    return parse_price_value(product.get("prime_price") or product.get("current_price"))


def regular_price_value(product):
    return parse_price_value(product.get("basis_price") or product.get("regular_price"))


def source_categories_text(product):
    return " ".join(product.get("source_categories") or []).lower()


def build_affinity_counts(keys, product_by_key):
    counts = {"categories": {}, "brands": {}, "tags": {}}
    for key in keys or []:
        product = product_by_key.get(key)
        if not product:
            continue
        category = effective_category(product)
        subcategory = effective_subcategory(product)
        brand = product.get("brand") or ""
        if category:
            counts["categories"][category] = counts["categories"].get(category, 0) + 1
        if subcategory:
            counts["categories"][subcategory] = counts["categories"].get(subcategory, 0) + 1
        if brand:
            counts["brands"][brand] = counts["brands"].get(brand, 0) + 1
        for tag in product.get("tags") or []:
            counts["tags"][tag] = counts["tags"].get(tag, 0) + 1
    return counts


def preferred_store_ids(profile):
    preferences = profile.get("newsletterPreferences") or {}
    explicit = preferences.get("preferredStoreIds") or []
    if explicit:
        return [str(value) for value in explicit if value]
    return [str(value) for value in (profile.get("selectedStoreIds") or []) if value]


def shelf_authority_score(product, *, profile=None, retailer_context="All", category_context=None):
    profile = profile or {}
    category = category_context or effective_category(product)
    subcategory = effective_subcategory(product)
    source_category_text = source_categories_text(product)
    product_name = normalize_text_key(product.get("name"))
    retailer = product.get("retailer") or "Whole Foods"
    available_store_ids = {str(value) for value in (product.get("available_store_ids") or []) if value}
    score = 0

    for store_id in preferred_store_ids(profile):
        if store_id and store_id in available_store_ids:
            score += 40
            break

    if retailer_context == "All" and retailer == "Whole Foods":
        score += 10
    elif retailer_context and retailer_context != "All" and retailer == retailer_context:
        score += 18

    if category == "Produce":
        if "fresh produce" in source_category_text:
            score += 260
        if "/produce/" in source_category_text or "/fruits" in source_category_text or "/vegetables" in source_category_text:
            score += 110
        if subcategory in {"Fruits", "Vegetables"}:
            score += 42
        elif subcategory in {"Cut Fruit & Veg", "Salad Greens", "Fresh Herbs", "Mushrooms"}:
            score += 26
        if retailer == "Whole Foods":
            score += 22
        if any(keyword in product_name for keyword in PRODUCE_EDGE_CASE_KEYWORDS):
            score -= 160
        return score

    if category == "Meat & Seafood":
        if any(fragment in source_category_text for fragment in ["/meat/", "/seafood/", "meat & seafood"]):
            score += 110
        if subcategory in {"Beef", "Pork", "Chicken", "Seafood"}:
            score += 18
        return score

    if category == "Prepared Foods":
        if any(fragment in source_category_text for fragment in ["instant food", "quick food", "deli", "prepared foods"]):
            score += 75
        return score

    if category == "Pantry":
        if any(fragment in source_category_text for fragment in ["/oil & seasoning", "/canned food", "/seaweed & dried produce/", "/pasta & dry goods/"]):
            score += 34
        return score

    if category == "Dairy & Eggs":
        if any(fragment in source_category_text for fragment in ["/milk/", "/eggs/", "/cheese/", "/yogurt/"]):
            score += 45
        return score

    return score


def value_score(product):
    discount_percent = int(product.get("discount_percent") or 0)
    score = discount_percent * 2.2
    current_price = current_price_value(product)
    regular_price = regular_price_value(product)
    if math.isfinite(current_price) and math.isfinite(regular_price) and regular_price > current_price:
        score += min(48, (regular_price - current_price) * 11)
    if product.get("prime_price"):
        score += 12
    if product.get("basis_price"):
        score += 8
    score += min(16, max(0, int(product.get("source_count") or 0) - 1) * 4)
    score += min(18, max(0.0, float(product.get("category_confidence") or 0)) * 18)
    if not discount_percent and not product.get("basis_price"):
        score -= 14
    return score


def preference_score(product, profile, liked, disliked):
    profile = profile or {}
    preferences = profile.get("newsletterPreferences") or {}
    category = effective_category(product)
    subcategory = effective_subcategory(product)
    brand = product.get("brand") or ""
    score = 0

    saved_keys = set(profile.get("savedKeys") or [])
    liked_keys = set(profile.get("likedKeys") or [])
    disliked_keys = set(profile.get("dislikedKeys") or [])
    preferred_categories = set(preferences.get("preferredCategories") or [])
    disliked_categories = set(preferences.get("dislikedCategories") or [])
    favorite_brands = {value.strip() for value in (preferences.get("favoriteBrands") or []) if str(value).strip()}
    hidden_brands = {value.strip() for value in (preferences.get("hiddenBrands") or []) if str(value).strip()}

    if product.get("key") in saved_keys:
        score += 120
    if product.get("key") in liked_keys:
        score += 72
    if product.get("key") in disliked_keys:
        score -= 180

    if category in preferred_categories:
        score += 42
    if subcategory and subcategory in preferred_categories:
        score += 28
    if category in disliked_categories or (subcategory and subcategory in disliked_categories):
        score -= 110

    if brand in favorite_brands:
        score += 38
    if brand in hidden_brands:
        score -= 120

    score += (liked["categories"].get(category, 0) or 0) * 18
    if subcategory:
        score += (liked["categories"].get(subcategory, 0) or 0) * 12
    score -= (disliked["categories"].get(category, 0) or 0) * 24
    if subcategory:
        score -= (disliked["categories"].get(subcategory, 0) or 0) * 14

    if brand:
        score += (liked["brands"].get(brand, 0) or 0) * 16
        score -= (disliked["brands"].get(brand, 0) or 0) * 22

    for tag in product.get("tags") or []:
        score += (liked["tags"].get(tag, 0) or 0) * 7
        score -= (disliked["tags"].get(tag, 0) or 0) * 9

    budget_mode = str(preferences.get("budgetSensitivity") or "").strip().lower()
    if budget_mode == "strong-deals" and int(product.get("discount_percent") or 0) >= 25:
        score += 18
    elif budget_mode == "premium-quality" and product.get("prime_price"):
        score += 10

    return score


def product_form_factor_key(product):
    name = normalize_text_key(product.get("name"))
    brand = normalize_text_key(product.get("brand"))
    tokens = [token for token in name.split() if token and token not in FORM_FACTOR_STOPWORDS and not any(char.isdigit() for char in token)]
    if brand:
        brand_tokens = set(brand.split())
        tokens = [token for token in tokens if token not in brand_tokens]
    if not tokens:
        return normalize_text_key(product.get("name"))
    return " ".join(tokens[:3])


def diversity_penalty(product, seen_counts):
    brand = product.get("brand") or ""
    subcategory = effective_subcategory(product)
    form_factor = product_form_factor_key(product)
    penalty = 0
    if brand:
        penalty += seen_counts["brands"].get(brand, 0) * 32
    if subcategory:
        penalty += seen_counts["subcategories"].get(subcategory, 0) * 18
    if form_factor:
        penalty += seen_counts["forms"].get(form_factor, 0) * 16
    return penalty


def recommendation_reason(product, profile):
    profile = profile or {}
    preferences = profile.get("newsletterPreferences") or {}
    brand = product.get("brand") or ""
    category = effective_category(product)
    if product.get("key") in set(profile.get("savedKeys") or []):
        return "Saved on your list"
    if brand and brand in set(preferences.get("favoriteBrands") or []):
        return f"You follow {brand}"
    if category in set(preferences.get("preferredCategories") or []):
        return f"You asked for more {category}"
    if "fresh produce" in source_categories_text(product) and category == "Produce":
        return "Strong fresh produce match"
    if int(product.get("discount_percent") or 0) >= 35:
        return "One of the strongest discounts today"
    return "Good fit for your feed"


def rank_products(products, *, profile=None, retailer_context="All", category_context=None):
    profile = profile or {}
    working = [dict(product) for product in (products or [])]
    product_by_key = {
        product.get("key") or product.get("asin") or product.get("product_key"): product
        for product in working
    }
    liked = build_affinity_counts(profile.get("likedKeys") or [], product_by_key)
    disliked = build_affinity_counts(profile.get("dislikedKeys") or [], product_by_key)

    scored = []
    for index, product in enumerate(working):
        key = product.get("key") or product.get("asin") or product.get("product_key") or f"product-{index}"
        product["key"] = key
        authority = shelf_authority_score(
            product,
            profile=profile,
            retailer_context=retailer_context,
            category_context=category_context,
        )
        personalization = preference_score(product, profile, liked, disliked)
        value = value_score(product)
        total = authority + personalization + value
        product["_rank"] = {
            "shelf_authority_score": round(authority, 2),
            "personalization_score": round(personalization, 2),
            "value_score": round(value, 2),
            "diversity_penalty": 0,
            "total_score": round(total, 2),
            "reason": recommendation_reason(product, profile),
        }
        scored.append(product)

    scored.sort(
        key=lambda product: (
            -(product["_rank"]["shelf_authority_score"]),
            -(product["_rank"]["personalization_score"]),
            -(product["_rank"]["value_score"]),
            normalize_text_key(product.get("name")),
        )
    )

    selected = []
    remaining = list(scored)
    seen_counts = {"brands": {}, "subcategories": {}, "forms": {}}
    while remaining:
        best_index = 0
        best_score = None
        for index, product in enumerate(remaining[:24]):
            penalty = diversity_penalty(product, seen_counts)
            adjusted = product["_rank"]["total_score"] - penalty
            if (
                best_score is None
                or adjusted > best_score
                or (
                    adjusted == best_score
                    and normalize_text_key(product.get("name")) < normalize_text_key(remaining[best_index].get("name"))
                )
            ):
                best_index = index
                best_score = adjusted
        chosen = remaining.pop(best_index)
        penalty = diversity_penalty(chosen, seen_counts)
        chosen["_rank"]["diversity_penalty"] = round(penalty, 2)
        chosen["_rank"]["total_score"] = round(chosen["_rank"]["total_score"] - penalty, 2)
        selected.append(chosen)
        brand = chosen.get("brand") or ""
        subcategory = effective_subcategory(chosen)
        form_factor = product_form_factor_key(chosen)
        if brand:
            seen_counts["brands"][brand] = seen_counts["brands"].get(brand, 0) + 1
        if subcategory:
            seen_counts["subcategories"][subcategory] = seen_counts["subcategories"].get(subcategory, 0) + 1
        if form_factor:
            seen_counts["forms"][form_factor] = seen_counts["forms"].get(form_factor, 0) + 1

    return selected
