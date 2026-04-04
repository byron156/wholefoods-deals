import json
import re
import requests
import os
import math
from flask import Flask, render_template

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DISCOVERED_DEALS_FILE = os.path.join(BASE_DIR, "discovered_products.json")
SEARCH_DEALS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")
FLYER_PRODUCTS_FILE = os.path.join(BASE_DIR, "flyer_products.json")
COMBINED_PRODUCTS_FILE = os.path.join(BASE_DIR, "combined_products.json")

app = Flask(__name__)

SALES_FLYER_URL = "https://www.wholefoodsmarket.com/sales-flyer?store-id=10160"


def emoji_for_product(name):
    if not name:
        return "🛒"

    name = name.lower()

    if "avocado" in name:
        return "🥑"
    if "milk" in name or "creamer" in name:
        return "🥛"
    if "chicken" in name:
        return "🍗"
    if "beef" in name or "lamb" in name or "ham" in name:
        return "🥩"
    if "salmon" in name or "fish" in name:
        return "🐟"
    if "shrimp" in name:
        return "🍤"
    if "strawberry" in name or "strawberries" in name:
        return "🍓"
    if "apple" in name:
        return "🍎"
    if "bread" in name:
        return "🍞"
    if "croissant" in name:
        return "🥐"
    if "cheese" in name:
        return "🧀"
    if "lemon" in name:
        return "🍋"
    if "orange" in name:
        return "🍊"
    if "mango" in name:
        return "🥭"
    if "kiwi" in name:
        return "🥝"
    if "broccoli" in name:
        return "🥦"
    if "egg" in name:
        return "🥚"
    if "dates" in name:
        return "🌴"
    if "quiche" in name:
        return "🥧"
    if "bacon" in name:
        return "🥓"
    if "potato" in name and "sweet" not in name:
        return "🥔"
    if "potato" in name and "sweet" in name:
        return "🍠"
    if "pancake" in name:
        return "🥞"
    if "juice" in name:
        return "🧃"
    if "rice" in name:
        return "🍚"
    if "cake" in name:
        return "🍰"
    if "chocolate" in name:
        return "🍫"
    if "blueberry" in name or "blueberries" in name:
        return "🫐"
    if "popcorn" in name:
        return "🍿"
    if "corn" in name:
        return "🌽"

    return "🛒"


def normalize_text_key(text):
    if not text:
        return ""

    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_next_data_from_html(html):
    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*type="application/json"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if match:
        return json.loads(match.group(1))

    match = re.search(
        r'<script[^>]*>\s*(\{.*?"pageProps".*?"promotions".*?"buildId".*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if match:
        return json.loads(match.group(1))

    raise ValueError("Could not find embedded sales flyer JSON in HTML.")


def round_up_cent(value):
    return math.ceil(value * 100) / 100


def is_variable_price(price_str):
    if not price_str:
        return False
    return "vary" in price_str.strip().lower()


def is_percent_off_text(text):
    if not text:
        return False
    return bool(re.search(r"\d+\s*%\s*off", text, re.IGNORECASE))


def is_buy_get_text(text):
    if not text:
        return False
    return bool(re.search(r"buy\s+\d+\s*,?\s*get\s+\d+\s+free", text, re.IGNORECASE))


def is_n_for_price_text(text):
    if not text:
        return False
    return bool(re.search(r"\d+\s+for\s+\$\d", text, re.IGNORECASE))


def is_non_price_promo_text(text):
    return (
        is_percent_off_text(text)
        or is_buy_get_text(text)
        or is_n_for_price_text(text)
    )


def clean_percent_text(text):
    if not text:
        return text

    match = re.search(r"(\d+)\s*%\s*off", text, re.IGNORECASE)
    if match:
        return f"{match.group(1)}% off"

    return text.strip()


def clean_discount_text(text):
    if not text:
        return None

    text = text.strip()
    if is_percent_off_text(text):
        return clean_percent_text(text)
    return text


def clean_regular_price_text(text):
    if not text:
        return None

    cleaned = text.strip()
    cleaned = re.sub(r"^\s*regular\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if "prices vary" in cleaned.lower():
        return "prices vary"

    if not cleaned:
        return None

    return cleaned


def extract_discount_sort_value(text):
    if not text:
        return -1

    matches = [int(value) for value in re.findall(r"(\d+)\s*%", text, re.IGNORECASE)]
    if not matches:
        return -1

    return max(matches)


def sort_products_for_display(products):
    ordered = list(products)
    ordered.sort(
        key=lambda product: (
            -extract_discount_sort_value(product.get("discount")),
            -product.get("source_count", 0),
            normalize_text_key(product.get("name")),
        )
    )
    return ordered


def format_price(value, suffix=""):
    if value is None:
        return None
    out = f"${value:.2f}"
    if suffix:
        out += suffix
    return out


def format_price_range(low, high, suffix=""):
    if low is None or high is None:
        return None
    return f"${low:.2f} to ${high:.2f}{suffix}"


def extract_single_price(price_str):
    if not price_str:
        return None, None

    s = price_str.strip()

    if (
        "buy " in s.lower()
        or " for $" in s.lower()
        or "%" in s.lower()
        or "vary" in s.lower()
        or " to " in s.lower()
    ):
        return None, None

    match = re.match(r'^\$(\d+(?:\.\d{1,2})?)(.*)$', s)
    if not match:
        return None, None

    value = float(match.group(1))
    suffix = match.group(2)
    return value, suffix


def extract_price_range(price_str):
    if not price_str:
        return None, None, None

    s = price_str.strip()
    match = re.match(r'^\$(\d+(?:\.\d{1,2})?)\s+to\s+\$(\d+(?:\.\d{1,2})?)(.*)$', s)
    if not match:
        return None, None, None

    low = float(match.group(1))
    high = float(match.group(2))
    suffix = match.group(3)
    return low, high, suffix


def extract_percent_off(price_str):
    if not price_str:
        return None

    match = re.search(r'(\d+)\s*%\s*off', price_str, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def extract_n_for_price(price_str):
    if not price_str:
        return None, None

    match = re.search(r'(\d+)\s+for\s+\$(\d+(?:\.\d{1,2})?)', price_str, re.IGNORECASE)
    if match:
        qty = int(match.group(1))
        total = float(match.group(2))
        return qty, total

    return None, None


def extract_buy_x_get_y(price_str):
    if not price_str:
        return None, None

    match = re.search(
        r'Buy\s+(\d+)\s*,?\s*Get\s+(\d+)\s+Free',
        price_str,
        re.IGNORECASE
    )
    if match:
        buy_qty = int(match.group(1))
        free_qty = int(match.group(2))
        return buy_qty, free_qty

    return None, None


def format_percent_range(low_pct, high_pct):
    low_pct = round(low_pct)
    high_pct = round(high_pct)

    if low_pct == high_pct:
        return f"{low_pct}% off"

    return f"{low_pct}% to {high_pct}% off"


def add_ea_if_needed(price_str):
    if not price_str:
        return price_str

    s = price_str.strip()
    lowered = s.lower()

    if is_non_price_promo_text(s):
        return clean_percent_text(s) if is_percent_off_text(s) else s

    if "prices vary" in lowered:
        return clean_regular_price_text(s)

    if "/lb" in lowered or " ea" in lowered or lowered.endswith("ea"):
        return s

    return s + " ea"


def infer_suffix_from_regular_price(regular_price):
    if not regular_price:
        return " ea"

    lowered = regular_price.strip().lower()

    if "/lb" in lowered:
        return "/lb"

    return " ea"


def ensure_price_has_suffix(price_str, regular_price=None):
    if not price_str:
        return price_str

    s = price_str.strip()
    lowered = s.lower()

    if is_non_price_promo_text(s):
        return clean_percent_text(s) if is_percent_off_text(s) else s

    if "prices vary" in lowered:
        return clean_regular_price_text(s)

    if "/lb" in lowered or " ea" in lowered or lowered.endswith("ea"):
        return s

    return s + infer_suffix_from_regular_price(regular_price)


def compute_discount_and_prime(regular_price, prime_price):
    display_prime_price = ensure_price_has_suffix(prime_price, regular_price)
    discount_text = None
    display_regular_price = add_ea_if_needed(regular_price)

    if is_variable_price(regular_price):
        return display_prime_price, discount_text, None

    regular_value, regular_suffix = extract_single_price(regular_price)
    regular_low, regular_high, range_suffix = extract_price_range(regular_price)
    prime_value, prime_suffix = extract_single_price(prime_price)
    prime_percent = extract_percent_off(prime_price)
    n_for_qty, n_for_total = extract_n_for_price(prime_price)
    buy_qty, free_qty = extract_buy_x_get_y(prime_price)

    if regular_value is not None and prime_value is not None and regular_value > 0:
        pct = 100 - 100 * (prime_value / regular_value)
        discount_text = f"{round(pct)}% off"
        return display_prime_price, discount_text, display_regular_price

    if regular_low is not None and regular_high is not None and prime_value is not None:
        low_pct = 100 - 100 * (prime_value / regular_low)
        high_pct = 100 - 100 * (prime_value / regular_high)
        discount_text = format_percent_range(min(low_pct, high_pct), max(low_pct, high_pct))
        return display_prime_price, discount_text, display_regular_price

    if regular_low is not None and regular_high is not None and prime_percent is not None:
        return clean_percent_text(display_prime_price), clean_percent_text(display_prime_price), display_regular_price

    if regular_value is not None and n_for_qty is not None and n_for_total is not None:
        per_item_prime = round_up_cent(n_for_total / n_for_qty)

        suffix = regular_suffix.strip() if regular_suffix else ""
        if not suffix:
            suffix = " ea"
        elif "ea" not in suffix.lower() and "/lb" not in suffix.lower():
            suffix = suffix + " ea"

        display_prime_price = format_price(per_item_prime, suffix)

        regular_suffix_display = regular_suffix.strip() if regular_suffix else ""
        if not regular_suffix_display:
            regular_suffix_display = " ea"
        elif "ea" not in regular_suffix_display.lower() and "/lb" not in regular_suffix_display.lower():
            regular_suffix_display = regular_suffix_display + " ea"

        display_regular_price = format_price(regular_value, regular_suffix_display)

        pct = 100 - 100 * (per_item_prime / regular_value)
        discount_text = f"{round(pct)}% off"
        return display_prime_price, discount_text, display_regular_price

    if regular_low is not None and regular_high is not None and n_for_qty is not None and n_for_total is not None:
        per_item_prime = round_up_cent(n_for_total / n_for_qty)

        suffix = range_suffix.strip() if range_suffix else ""
        if not suffix:
            suffix = " ea"
        elif "ea" not in suffix.lower() and "/lb" not in suffix.lower():
            suffix = suffix + " ea"

        display_prime_price = format_price(per_item_prime, suffix)

        regular_suffix_display = range_suffix.strip() if range_suffix else ""
        if not regular_suffix_display:
            regular_suffix_display = " ea"
        elif "ea" not in regular_suffix_display.lower() and "/lb" not in regular_suffix_display.lower():
            regular_suffix_display = regular_suffix_display + " ea"

        display_regular_price = format_price_range(
            regular_low,
            regular_high,
            regular_suffix_display
        )

        low_pct = 100 - 100 * (per_item_prime / regular_low)
        high_pct = 100 - 100 * (per_item_prime / regular_high)
        discount_text = format_percent_range(min(low_pct, high_pct), max(low_pct, high_pct))
        return display_prime_price, discount_text, display_regular_price

    if regular_value is not None and buy_qty is not None and free_qty is not None:
        total_units = buy_qty + free_qty
        per_item_prime = round_up_cent((regular_value * buy_qty) / total_units)
        suffix = regular_suffix.strip() if regular_suffix else ""
        if not suffix:
            suffix = " ea"
        elif "ea" not in suffix.lower() and "/lb" not in suffix.lower():
            suffix = suffix + " ea"
        display_prime_price = format_price(per_item_prime, suffix)
        pct = 100 * (free_qty / total_units)
        discount_text = f"{round(pct)}% off"
        return display_prime_price, discount_text, display_regular_price

    if regular_low is not None and regular_high is not None and buy_qty is not None and free_qty is not None:
        total_units = buy_qty + free_qty
        per_item_low = round_up_cent((regular_low * buy_qty) / total_units)
        per_item_high = round_up_cent((regular_high * buy_qty) / total_units)
        suffix = range_suffix.strip() if range_suffix else ""
        if not suffix:
            suffix = " ea"
        elif "ea" not in suffix.lower() and "/lb" not in suffix.lower():
            suffix = suffix + " ea"
        display_prime_price = format_price_range(per_item_low, per_item_high, suffix)
        pct = 100 * (free_qty / total_units)
        discount_text = f"{round(pct)}% off"
        return display_prime_price, discount_text, display_regular_price

    if prime_percent is not None and regular_value is not None:
        derived_prime = regular_value * (1 - prime_percent / 100)

        suffix = regular_suffix.strip() if regular_suffix else ""
        if "/lb" not in suffix.lower():
            if not suffix:
                suffix = " ea"
            elif "ea" not in suffix.lower():
                suffix = suffix + " ea"

        display_prime_price = format_price(derived_prime, suffix)
        discount_text = f"{prime_percent}% off"
        return display_prime_price, discount_text, display_regular_price

    if prime_percent is not None and regular_low is not None and regular_high is not None:
        derived_low = round_up_cent(regular_low * (1 - prime_percent / 100))
        derived_high = round_up_cent(regular_high * (1 - prime_percent / 100))

        suffix = range_suffix.strip() if range_suffix else ""
        if not suffix:
            suffix = " ea"
        elif "ea" not in suffix.lower() and "/lb" not in suffix.lower():
            suffix = suffix + " ea"

        display_prime_price = format_price_range(derived_low, derived_high, suffix)
        discount_text = f"{prime_percent}% off"
        return display_prime_price, discount_text, display_regular_price

    if display_regular_price and display_prime_price:
        regular_num, _ = extract_single_price(display_regular_price)
        prime_num, _ = extract_single_price(display_prime_price)
        if regular_num is not None and prime_num is not None and regular_num > 0:
            pct = 100 - 100 * (prime_num / regular_num)
            discount_text = f"{round(pct)}% off"
            return display_prime_price, discount_text, display_regular_price

    if is_percent_off_text(prime_price):
        clean = clean_percent_text(prime_price)
        return clean, clean, display_regular_price

    if not discount_text and display_regular_price and display_prime_price:
        discount_text = "0% off"

    return display_prime_price, discount_text, display_regular_price


def resolve_display_pricing(regular_price=None, prime_price=None, current_price=None, discount_text=None):
    candidate_regular = clean_regular_price_text(regular_price)
    candidate_prime = prime_price or current_price
    candidate_discount = clean_discount_text(discount_text)
    candidate_current = current_price

    promo_candidates = [
        clean_discount_text(candidate_discount),
        clean_discount_text(candidate_prime if is_percent_off_text(candidate_prime) else None),
        clean_discount_text(candidate_current if is_percent_off_text(candidate_current) else None),
    ]
    promo_candidates = [value for value in promo_candidates if value and value != "0% off"]
    strongest_promo = None
    if promo_candidates:
        strongest_promo = max(promo_candidates, key=extract_discount_sort_value)

    if candidate_prime and is_percent_off_text(candidate_prime):
        display_regular_price = add_ea_if_needed(candidate_regular) if candidate_regular else None
        return display_regular_price, None, strongest_promo

    if candidate_regular and candidate_prime:
        display_prime_price, computed_discount, display_regular_price = compute_discount_and_prime(
            candidate_regular,
            candidate_prime,
        )

        if not display_regular_price:
            display_regular_price = add_ea_if_needed(candidate_regular or current_price or candidate_prime)

        if computed_discount == "0% off":
            computed_discount = None

        if display_regular_price and display_prime_price:
            if normalize_text_key(display_regular_price) == normalize_text_key(display_prime_price):
                display_regular_price = None

        return display_regular_price, display_prime_price, computed_discount

    if candidate_regular and candidate_discount:
        display_prime_price, computed_discount, display_regular_price = compute_discount_and_prime(
            candidate_regular,
            candidate_discount,
        )

        if not display_regular_price:
            display_regular_price = add_ea_if_needed(candidate_regular)

        final_discount = computed_discount or candidate_discount
        if final_discount == "0% off":
            final_discount = None

        if display_regular_price and display_prime_price:
            if normalize_text_key(display_regular_price) == normalize_text_key(display_prime_price):
                display_regular_price = None

        return display_regular_price, display_prime_price, final_discount

    if candidate_regular:
        return add_ea_if_needed(candidate_regular), None, strongest_promo or candidate_discount

    if candidate_prime:
        if is_percent_off_text(candidate_prime):
            return None, None, strongest_promo or clean_percent_text(candidate_prime)
        return None, ensure_price_has_suffix(candidate_prime, regular_price), strongest_promo or candidate_discount

    return None, None, strongest_promo or candidate_discount


def standardize_product_record(
    *,
    name,
    image=None,
    url=None,
    asin=None,
    asins=None,
    regular_price=None,
    prime_price=None,
    current_price=None,
    discount_text=None,
    unit_price=None,
    emoji=None,
    extra_fields=None,
):
    display_regular_price, display_prime_price, final_discount = resolve_display_pricing(
        regular_price=regular_price,
        prime_price=prime_price,
        current_price=current_price,
        discount_text=discount_text,
    )

    product = {
        "asin": asin,
        "name": name,
        "image": image,
        "url": url,
        "unit_price": unit_price,
        "current_price": current_price,
        "basis_price": display_regular_price,
        "prime_price": display_prime_price,
        "discount": final_discount,
        "emoji": emoji or emoji_for_product(name),
    }

    if asins:
        product["asins"] = asins
        if not product.get("asin"):
            product["asin"] = str(asins[0]).strip()

    if extra_fields:
        product.update(extra_fields)

    return product


def load_all_deals():
    print("Looking for all-deals file at:", DISCOVERED_DEALS_FILE)

    try:
        with open(DISCOVERED_DEALS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("File not found.")
        return []

    print("Loaded", len(raw_products), "all-deals products")

    products = []

    for p in raw_products:
        products.append(
            standardize_product_record(
                asin=p.get("asin"),
                name=p.get("name"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                emoji=p.get("emoji"),
            )
        )

    return products


def load_search_deals():
    print("Looking for search-deals file at:", SEARCH_DEALS_FILE)

    try:
        with open(SEARCH_DEALS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("Search-deals file not found.")
        return []

    print("Loaded", len(raw_products), "search-deals products")

    products = []

    for p in raw_products:
        products.append(
            standardize_product_record(
                asin=p.get("asin"),
                name=p.get("name"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                emoji=p.get("emoji"),
            )
        )

    return products


def load_saved_flyer_products():
    print("Looking for flyer products file at:", FLYER_PRODUCTS_FILE)

    try:
        with open(FLYER_PRODUCTS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("Flyer products file not found; falling back to live fetch.")
        return fetch_products()

    print("Loaded", len(raw_products), "flyer products")

    products = []
    for p in raw_products:
        products.append(
            standardize_product_record(
                asin=p.get("asin"),
                asins=p.get("asins"),
                name=p.get("name"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price") or p.get("sale_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                emoji=p.get("emoji"),
                extra_fields={"rank": p.get("rank"), "sale_price": p.get("sale_price")},
            )
        )

    products.sort(key=lambda x: x["rank"] if x.get("rank") is not None else 9999)
    return products


def merge_combined_product(existing, incoming):
    if not existing:
        merged = dict(incoming)
        merged["sources"] = list(incoming.get("sources", []))
        merged["source_count"] = len(merged["sources"])
        return merged

    merged = dict(existing)

    for key, value in incoming.items():
        if key == "sources":
            continue
        if value in (None, "", []):
            continue
        if merged.get(key) in (None, "", []):
            merged[key] = value

    merged_sources = []
    for source in existing.get("sources", []) + incoming.get("sources", []):
        if source not in merged_sources:
            merged_sources.append(source)

    merged["sources"] = merged_sources
    merged["source_count"] = len(merged_sources)
    return merged


def combined_key_for_product(product):
    asins = product.get("asins") or []
    asins = sorted(str(asin).strip() for asin in asins if asin)
    if len(asins) > 1:
        return "asins:" + ",".join(asins)

    asin = product.get("asin")
    if asin:
        return f"asin:{asin}"

    if asins:
        return f"asin:{asins[0]}"

    return "name:" + normalize_text_key(product.get("name"))


def normalized_product_for_source(product, source_name):
    normalized = standardize_product_record(
        asin=product.get("asin"),
        asins=product.get("asins"),
        name=product.get("name"),
        image=product.get("image"),
        url=product.get("url"),
        unit_price=product.get("unit_price"),
        current_price=product.get("current_price") or product.get("sale_price"),
        regular_price=product.get("basis_price"),
        prime_price=product.get("prime_price"),
        discount_text=product.get("discount"),
        emoji=product.get("emoji"),
    )
    normalized["sources"] = [source_name]
    return normalized


def build_combined_products(flyer_products, all_deals_products, search_deals_products):
    combined = {}

    datasets = [
        ("Flyer", flyer_products),
        ("All Deals", all_deals_products),
        ("Search Deals", search_deals_products),
    ]

    for source_name, products in datasets:
        for product in products:
            normalized = normalized_product_for_source(product, source_name)
            key = combined_key_for_product(normalized)
            combined[key] = merge_combined_product(combined.get(key), normalized)

    ordered = list(combined.values())
    ordered.sort(
        key=lambda product: (
            -product.get("source_count", 0),
            normalize_text_key(product.get("name")),
        )
    )
    return ordered


def load_combined_products():
    print("Looking for combined products file at:", COMBINED_PRODUCTS_FILE)

    try:
        with open(COMBINED_PRODUCTS_FILE, "r", encoding="utf-8") as f:
            products = json.load(f)
    except FileNotFoundError:
        print("Combined products file not found; rebuilding from current sources.")
        flyer_products = load_saved_flyer_products()
        all_deals_products = load_all_deals()
        search_deals_products = load_search_deals()
        return build_combined_products(flyer_products, all_deals_products, search_deals_products)

    print("Loaded", len(products), "combined products")
    normalized_products = []
    for p in products:
        normalized = standardize_product_record(
            asin=p.get("asin"),
            asins=p.get("asins"),
            name=p.get("name"),
            image=p.get("image"),
            url=p.get("url"),
            unit_price=p.get("unit_price"),
            current_price=p.get("current_price") or p.get("sale_price"),
            regular_price=p.get("basis_price"),
            prime_price=p.get("prime_price"),
            discount_text=p.get("discount"),
            emoji=p.get("emoji"),
        )
        sources = list(p.get("sources") or [])
        normalized["sources"] = sources
        normalized["source_count"] = len(sources)
        normalized_products.append(normalized)

    return normalized_products


def fetch_products():
    r = requests.get(SALES_FLYER_URL, timeout=20)
    r.raise_for_status()

    next_data = extract_next_data_from_html(r.text)

    promotions = (
        next_data.get("props", {}).get("pageProps", {}).get("promotions")
        or next_data.get("pageProps", {}).get("promotions")
        or []
    )

    products = []

    for p in promotions:
        products.append(
            standardize_product_record(
                name=p.get("productName", "Unknown Product"),
                image=p.get("productImage"),
                regular_price=p.get("regularPrice"),
                current_price=p.get("salePrice"),
                prime_price=p.get("primePrice"),
                asins=p.get("asinsList", []),
                emoji=emoji_for_product(p.get("productName", "Unknown Product")),
                extra_fields={"rank": p.get("rank"), "sale_price": p.get("salePrice")},
            )
        )

    products.sort(key=lambda x: x["rank"] if x["rank"] is not None else 9999)
    return products


def validate_product_fields(products, label="products"):
    required_fields = ["image", "emoji", "name", "prime_price", "basis_price", "discount"]
    problems = []

    for i, p in enumerate(products):
        missing = [field for field in required_fields if not p.get(field)]
        if missing:
            problems.append({
                "index": i,
                "name": p.get("name"),
                "asin": p.get("asin"),
                "missing": missing,
            })

    print(f"{label}: {len(products)} products checked")
    print(f"{label}: {len(problems)} products with missing required fields")

    for row in problems[:50]:
        print(
            "-",
            row.get("name") or "(no name)",
            "| ASIN:",
            row.get("asin") or "(none)",
            "| missing:",
            ", ".join(row["missing"]),
        )

    return problems


@app.route("/")
def combined_products_home():
    products = sort_products_for_display(load_combined_products())
    deal_count = len(products)
    return render_template(
        "combined_products.html",
        products=products,
        deal_count=deal_count,
        page_subtitle="Search combined products across flyer, all deals, and search deals",
    )


@app.route("/flyer")
def deals():
    products = sort_products_for_display(load_saved_flyer_products())
    deal_count = len(products)
    return render_template(
        "deals.html",
        products=products,
        deal_count=deal_count,
        page_subtitle="Search products across flyer",
    )


@app.route("/newsletter")
def newsletter():
    products = load_saved_flyer_products()
    deal_count = len(products)
    return render_template("newsletter.html", products=products, deal_count=deal_count)


@app.route("/all-deals")
def all_deals():
    products = sort_products_for_display(load_all_deals())
    deal_count = len(products)
    return render_template(
        "all_deals.html",
        products=products,
        deal_count=deal_count,
        page_subtitle="Search products across all deals",
    )


@app.route("/search-deals")
def search_deals():
    products = sort_products_for_display(load_search_deals())
    deal_count = len(products)
    return render_template(
        "search_deals.html",
        products=products,
        deal_count=deal_count,
        page_subtitle="Search products across search deals",
    )


@app.route("/all-deals-newsletter")
def all_deals_newsletter():
    products = load_all_deals()
    deal_count = len(products)
    return render_template("all_deals_newsletter.html", products=products, deal_count=deal_count)


if __name__ == "__main__":
    app.run(debug=True)
