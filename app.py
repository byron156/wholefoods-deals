import json
import re
import requests
import os
import math
from flask import Flask, jsonify, render_template, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DISCOVERED_DEALS_FILE = os.path.join(BASE_DIR, "discovered_products.json")
SEARCH_DEALS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")
FLYER_PRODUCTS_FILE = os.path.join(BASE_DIR, "flyer_products.json")
COMBINED_PRODUCTS_FILE = os.path.join(BASE_DIR, "combined_products.json")
TARGET_DEALS_FILE = os.path.join(BASE_DIR, "target_deals_products.json")
HMART_DEALS_FILE = os.path.join(BASE_DIR, "hmart_deals_products.json")

app = Flask(__name__)

SALES_FLYER_URL = "https://www.wholefoodsmarket.com/sales-flyer?store-id=10160"
SUPPORTED_STORES = [
    {
        "id": "10160",
        "slug": "columbus-circle",
        "name": "Columbus Circle",
        "city": "New York",
        "state": "NY",
        "label": "Columbus Circle, NYC",
        "is_active": True,
    }
]
DEFAULT_STORE_IDS = [SUPPORTED_STORES[0]["id"]]
CATEGORY_PROFILES = {
    "Produce": {
        "strong": [
            "fresh fruit", "fresh vegetable", "salad kit", "salad mix", "baby spinach",
            "romaine", "broccoli florets", "cauliflower florets", "avocado", "apple",
            "banana", "grapes", "berries", "blueberries", "strawberries", "raspberries",
            "blackberries", "cherries", "fresh cherries", "citrus", "lettuce", "tomato", "onion", "carrot", "mango",
            "kiwi", "pear", "peach", "plum", "melon", "pineapple", "fresh mushroom", "mushroom", "mushrooms",
        ],
        "medium": ["produce", "fruit", "vegetable", "salad", "greens", "herbs"],
        "weak": ["cherry", "lemon", "orange", "lime"],
        "exclude": [
            "cherry cola", "cherry gummies", "gummy", "candy", "chocolate", "cookie",
            "granola bar", "protein bar", "sparkling", "seltzer", "juice box",
            "mushroom coffee", "mushroom powder", "mushroom supplement",
        ],
    },
    "Meat & Seafood": {
        "strong": [
            "chicken", "beef", "steak", "ground beef", "salmon", "tuna", "fish", "shrimp",
            "bacon", "turkey", "ham", "lamb", "sausage", "pork", "scallop", "seafood",
            "crab", "lobster", "meatballs", "cutlet",
        ],
        "medium": ["meat", "seafood", "poultry", "jerky"],
        "weak": [],
        "exclude": ["dog food"],
    },
    "Dairy & Eggs": {
        "strong": [
            "milk", "cheese", "butter", "yogurt", "egg", "cream cheese", "kefir",
            "cottage cheese", "sour cream", "half and half", "cream", "mozzarella",
            "cheddar", "feta", "parmesan",
        ],
        "medium": ["dairy", "creamer"],
        "weak": [],
        "exclude": ["ice cream", "frozen dessert", "seed butter", "sunflower butter", "almond butter", "cashew butter", "nut butter", "peanut butter", "chocolate egg", "chocolate eggs", "queso", "alfredo", "macaroni", "macaroni and cheese", "mac cheese", "shells and cheddar"],
    },
    "Bakery": {
        "strong": [
            "bread", "biscuit", "croissant", "cake", "muffin", "bagel", "cookie", "pie",
            "pastry", "brownie", "donut", "tortilla", "bun", "roll", "scone",
        ],
        "medium": ["bakery", "baked"],
        "weak": [],
        "exclude": ["ice cream cake", "pancake mix", "waffle mix"],
    },
    "Prepared Foods": {
        "strong": [
            "kimchi", "kimbap", "gimbap", "banchan", "side dish", "sidedish", "deli",
            "dumpling", "mandu", "tteokbokki", "katsu", "ready meal", "prepared meal",
            "instant food", "rice bowl", "fried rice", "sushi", "meal kit",
        ],
        "medium": ["prepared", "quick food", "heat and eat", "ready to eat"],
        "weak": [],
        "exclude": ["dish soap", "laundry", "pet food"],
    },
    "Frozen": {
        "strong": [
            "frozen", "ice cream", "gelato", "pizza", "waffle", "popsicle", "sorbet",
            "frozen dessert", "ice pop", "frozen fruit", "frozen vegetable",
        ],
        "medium": ["freezer"],
        "weak": [],
        "exclude": [],
    },
    "Snacks": {
        "strong": [
            "chips", "cracker", "pretzel", "popcorn", "snack", "granola bar", "candy",
            "chocolate", "bites", "crisps", "gummy", "trail mix", "snack bar",
            "protein bar", "fruit snacks", "cookies", "coconut chips", "chocolate egg", "chocolate eggs",
        ],
        "medium": ["bar", "jerky", "nuts", "chews"],
        "weak": ["cherry", "berry"],
        "exclude": ["broth", "protein powder", "dish soap"],
    },
    "Pantry": {
        "strong": [
            "pasta", "rice", "sauce", "vinegar", "oil", "flour", "spice", "seasoning",
            "broth", "beans", "soup", "hummus", "bruschetta", "oatmeal", "cereal",
            "granola", "peanut butter", "jam", "honey", "mustard", "ketchup", "marinade",
            "dressing", "salsa", "fruit spread", "spread", "preserves", "seed butter",
            "sunflower butter", "almond butter", "cashew butter", "nut butter", "hommus",
            "queso", "alfredo", "macaroni", "macaroni and cheese", "mac cheese", "shells and cheddar",
        ],
        "medium": ["pantry", "mix", "canned", "jarred"],
        "weak": [],
        "exclude": ["cake", "cookie", "chips", "sparkling water"],
    },
    "Beverages": {
        "strong": [
            "coffee", "tea", "juice", "water", "seltzer", "soda", "kombucha", "smoothie",
            "ipa", "beer", "wine", "latte", "drink", "cold brew", "sparkling water",
            "energy drink", "coconut water", "hard seltzer", "budweiser", "bud light",
            "bota box", "cabernet", "merlot", "chardonnay", "riesling", "pinot",
            "sauvignon", "prosecco", "stella", "modelo", "corona", "heineken",
            "coors", "michelob", "black stallion", "pale ale", "ale", "spritz",
            "spiked", "zero proof", "non alcoholic", "non-alcoholic",
        ],
        "medium": ["beverage"],
        "weak": [],
        "exclude": ["drink mix", "drinkware"],
    },
    "Supplements & Wellness": {
        "strong": [
            "vitamin", "supplement", "enzyme", "probiotic", "collagen", "magnesium",
            "omega", "capsule", "wellness", "multivitamin", "shots", "shot", "peptides",
            "digestive", "powder", "electrolyte", "turmeric", "elixir", "calcium",
        ],
        "medium": ["protein", "greens powder", "wellness", "tonic", "adaptogen"],
        "weak": [],
        "exclude": ["protein bar", "shot glass"],
    },
    "Household": {
        "strong": [
            "detergent", "cleaner", "soap refill", "paper towel", "trash bag",
            "laundry", "toilet paper", "clean day", "disinfect", "sponge",
        ],
        "medium": ["household", "cleaning"],
        "weak": [],
        "exclude": ["dish pizza", "soap bar", "side dish", "sidedish", "kimchi", "deli", "dumpling"],
    },
    "Beauty & Personal Care": {
        "strong": [
            "shampoo", "conditioner", "deodorant", "lotion", "serum", "hand soap",
            "body wash", "toothpaste", "mouthwash", "cleanser", "moisturizer",
            "lip balm", "sunscreen",
        ],
        "medium": ["beauty", "personal care", "soap"],
        "weak": [],
        "exclude": ["dish soap", "laundry soap"],
    },
}
SUBCATEGORY_PROFILES = {
    "Produce": {
        "Fruit": ["apple", "banana", "berries", "berry", "grapes", "grape", "cherries", "cherry", "citrus", "orange", "lemon", "lime", "kiwi", "mango", "pear", "peach", "plum", "melon", "pineapple", "strawberries", "blueberries", "raspberries", "blackberries"],
        "Vegetables": ["broccoli", "cauliflower", "lettuce", "tomato", "onion", "carrot", "pepper", "cucumber", "avocado", "potato", "sweet potato", "kale", "spinach"],
        "Salads & Greens": ["salad", "greens", "romaine", "baby spinach", "spring mix", "salad kit", "salad mix"],
        "Herbs": ["herb", "cilantro", "parsley", "basil", "mint"],
        "Mushrooms": ["mushroom", "mushrooms", "shiitake", "lion's mane", "lions mane", "oyster mushroom"],
    },
    "Meat & Seafood": {
        "Chicken & Turkey": ["chicken", "turkey", "cutlet", "breast", "thigh", "drumstick"],
        "Beef, Pork & Lamb": ["beef", "pork", "lamb", "ham", "steak", "sausage", "bacon", "ground beef"],
        "Seafood": ["salmon", "tuna", "shrimp", "fish", "seafood", "crab", "lobster", "scallop"],
        "Deli & Prepared Meat": ["meatballs", "deli", "prosciutto", "pepperoni"],
        "Sausages & Meatballs": ["sausage", "sausages", "meatballs", "meatball"],
    },
    "Dairy & Eggs": {
        "Milk & Creamers": ["milk", "creamer", "half and half", "kefir"],
        "Cheese": ["cheese", "mozzarella", "cheddar", "feta", "parmesan", "cream cheese", "cottage cheese"],
        "Yogurt & Cultured Dairy": ["yogurt", "kefir", "cultured"],
        "Eggs & Butter": ["egg", "butter", "sour cream"],
    },
    "Bakery": {
        "Bread & Bagels": ["bread", "bagel", "bun", "roll", "tortilla", "wrap"],
        "Pastries & Desserts": ["croissant", "cake", "muffin", "pie", "pastry", "brownie", "donut", "scone"],
        "Cookies & Biscuits": ["cookie", "biscuit", "cracker biscuit"],
    },
    "Prepared Foods": {
        "Kimchi & Sides": ["kimchi", "banchan", "side dish", "sidedish", "deli"],
        "Rice Meals & Kimbap": ["kimbap", "gimbap", "rice bowl", "fried rice", "sushi"],
        "Dumplings & Quick Meals": ["dumpling", "mandu", "tteokbokki", "katsu", "instant food", "ready meal"],
    },
    "Frozen": {
        "Ice Cream & Desserts": ["ice cream", "gelato", "sorbet", "frozen dessert", "ice pop", "popsicle"],
        "Frozen Meals & Pizza": ["pizza", "frozen meal", "dumpling", "entree"],
        "Frozen Produce": ["frozen fruit", "frozen vegetable"],
        "Frozen Breakfast": ["frozen waffle", "waffle", "frozen pancake"],
    },
    "Snacks": {
        "Candy & Gummies": ["candy", "gummy", "fruit snacks", "chews", "chocolate"],
        "Chips & Crackers": ["chips", "cracker", "pretzel", "popcorn", "crisps"],
        "Cookies & Sweet Snacks": ["cookies", "cookie", "bites"],
        "Bars": ["granola bar", "protein bar", "snack bar", "bar"],
        "Nuts & Trail Mix": ["nuts", "trail mix", "almonds", "cashews", "pistachio"],
    },
    "Pantry": {
        "Pasta, Rice & Grains": ["pasta", "rice", "oatmeal", "cereal", "granola", "flour"],
        "Sauces, Broth & Soup": ["sauce", "broth", "soup", "marinade", "dressing", "salsa"],
        "Condiments & Spreads": ["peanut butter", "jam", "fruit spread", "spread", "preserves", "honey", "mustard", "ketchup", "vinegar", "oil"],
        "Canned & Jarred Goods": ["beans", "jarred", "canned", "bruschetta", "hummus"],
        "Baking & Seasonings": ["spice", "seasoning", "mix"],
        "Dips & Spreads": ["hummus", "hommus", "dip", "dips", "guacamole", "queso"],
    },
    "Beverages": {
        "Water & Seltzer": ["water", "seltzer", "sparkling water", "coconut water"],
        "Coffee & Tea": ["coffee", "tea", "latte", "cold brew"],
        "Juice & Smoothies": ["juice", "smoothie", "kombucha"],
        "Beer, Wine & Spirits": ["ipa", "beer", "wine", "hard seltzer", "lager"],
        "Energy & Sports Drinks": ["energy drink", "electrolyte", "sports drink"],
        "Mocktails & Zero Proof": ["zero proof", "non-alcoholic", "non alcoholic", "mocktail", "ritual"],
    },
    "Supplements & Wellness": {
        "Vitamins & Minerals": ["vitamin", "magnesium", "omega", "multivitamin"],
        "Digestive & Probiotics": ["enzyme", "digestive", "probiotic"],
        "Protein & Collagen": ["collagen", "protein powder", "peptides", "greens powder"],
        "Wellness Shots & Tonics": ["wellness shot", "wellness shots", "shot", "tonic", "electrolyte"],
        "Mushroom Blends": ["functional mushroom", "lion's mane", "cordyceps", "reishi", "chaga", "mushroom powder"],
    },
    "Household": {
        "Cleaning": ["cleaner", "disinfect", "sponge"],
        "Dish & Laundry": ["laundry", "detergent", "dish soap"],
        "Paper & Trash": ["paper towel", "toilet paper", "trash bag"],
    },
    "Beauty & Personal Care": {
        "Hair Care": ["shampoo", "conditioner"],
        "Skin & Body Care": ["lotion", "serum", "body wash", "moisturizer", "cleanser", "sunscreen", "lip balm"],
        "Oral Care": ["toothpaste", "mouthwash"],
        "Soap & Deodorant": ["hand soap", "soap bar", "deodorant"],
    },
}

DIRECT_CATEGORY_HINTS = [
    {
        "category": "Pantry",
        "include": ["macaroni and cheese", "mac and cheese", "mac cheese", "shells and cheddar"],
        "exclude": ["frozen", "pizza"],
    },
    {
        "category": "Prepared Foods",
        "include": ["kimchi", "kimbap", "gimbap", "banchan", "side dish", "sidedish", "dumpling", "mandu", "tteokbokki"],
        "exclude": ["dish soap", "laundry"],
    },
    {
        "category": "Beverages",
        "include": [
            "budweiser", "bud light", "bota box", "black stallion", "hard seltzer", "beer",
            "wine", "lager", "ipa", "stout", "pilsner", "cabernet", "merlot", "chardonnay",
            "riesling", "pinot", "sauvignon", "prosecco", "rose", "rosé", "ale", "spritz",
            "spiked", "non alcoholic", "non-alcoholic", "zero proof",
        ],
        "exclude": ["drink mix", "wine vinegar"],
    },
]
TAG_KEYWORDS = {
    "organic": ["organic"],
    "vegan": ["vegan"],
    "vegetarian": ["vegetarian"],
    "gluten-free": ["gluten free", "gluten-free"],
    "keto": ["keto"],
    "paleo": ["paleo"],
    "non-gmo": ["non-gmo", "non gmo"],
    "dairy-free": ["dairy free", "dairy-free"],
    "high-protein": ["protein"],
}


def text_contains_phrase(haystack, phrase):
    phrase_key = normalize_text_key(phrase)
    if not phrase_key:
        return False

    variants = {phrase_key}
    tokens = phrase_key.split()
    if tokens:
        last = tokens[-1]
        plural_variants = set()
        if last.endswith("y") and len(last) > 1:
            plural_variants.add(last[:-1] + "ies")
        if last.endswith(("s", "x", "z", "ch", "sh")):
            plural_variants.add(last + "es")
        else:
            plural_variants.add(last + "s")

        for variant_last in plural_variants:
            variants.add(" ".join(tokens[:-1] + [variant_last]).strip())

    haystack_padded = f" {haystack} "
    return any(f" {variant} " in haystack_padded for variant in variants if variant)


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
            -(product.get("discount_percent") or extract_discount_sort_value(product.get("discount"))),
            -(1 if product.get("prime_price") else 0),
            -(1 if product.get("basis_price") else 0),
            -product.get("source_count", 0),
            -(product.get("category_confidence") or 0),
            normalize_text_key(product.get("name")),
        )
    )
    return ordered


def derive_brand(name, explicit_brand=None):
    if explicit_brand:
        cleaned_explicit_brand = trim_brand_candidate(explicit_brand.strip())
        normalized_explicit_brand = normalize_text_key(cleaned_explicit_brand)
        if "suppliers may vary" in normalized_explicit_brand:
            return None
        if candidate_is_generic_brand(cleaned_explicit_brand):
            return None
        return cleaned_explicit_brand

    if not name:
        return None

    canonical = canonical_brand_for_alias(name)
    if canonical:
        return canonical

    brand_candidate = extract_brand_candidate(name)
    if brand_candidate:
        return trim_brand_candidate(brand_candidate)

    return None


SMALL_TITLE_WORDS = {
    "a", "an", "and", "as", "at", "by", "for", "from", "in", "of", "on", "or", "the", "to", "with",
}
DISPLAY_NAME_CLAIM_MARKERS = [
    "non-gmo", "gluten-free", "gluten free", "dairy-free", "pork-free", "no nitrates", "no antibiotics",
    "fully cooked", "natural ingredients", "for immune", "for stress", "gentle on", "energy", "focus",
    "mental clarity", "support", "clinically tested", "servings", "tablets", "capsules", "raised with",
]
GENERIC_REMAINDER_NAMES = {
    "beer", "wine", "coffee", "items", "item", "products", "product", "brands", "brand", "snacks",
}
BRAND_CONNECTORS = {"&", "and", "of", "by", "the", "foods", "food", "company", "co", "superfood"}
BRAND_STOP_WORDS = {
    "select", "fresh", "organic", "sale", "buy", "chicken", "pork", "beef", "turkey", "coffee", "tea",
    "water", "juice", "milk", "cheese", "yogurt", "bread", "chips", "snacks", "meatballs", "sausages",
    "burrata", "mascarpone", "multivitamin", "vitamin", "supplement", "cucumbers", "oranges", "potatoes",
}
GENERIC_BRAND_WORDS = {
    "ancient", "baby", "bacon", "bar", "basil", "berry", "biscuits", "black", "blend", "block", "broth",
    "buttermilk", "candy", "capsules", "care", "cheese", "chicken", "chips", "coffee", "collagen", "cookies",
    "cracker", "cracked", "cream", "daily", "deodorant", "dessert", "digestive", "dip", "dressings", "drink",
    "electrolyte", "elixir", "enzyme", "enzymes", "extract", "farm", "feta", "flour", "foods", "fresh", "fruit",
    "gels", "granola", "greens", "hair", "herbal", "hummus", "immune", "items", "jerky", "juice", "jumbo", "latte",
    "large", "loaf", "lotion", "mane", "mango", "meatballs", "melon", "mental", "milk", "mix", "moss", "muffins", "mushroom",
    "oatmeal", "on", "organic", "pasta", "peptides", "pepper", "pork", "potato", "powder", "probiotic", "probiotics",
    "produce", "protein", "queso", "recovery", "rice", "salmon", "salt", "seasoning", "serum", "shell", "shots", "shrimp", "single",
    "small", "snacks", "soap", "source", "spread", "stress", "supplement", "supplements", "superfood", "tablets", "tea",
    "tonic", "turkey", "vanilla", "variety", "vitamin", "vitamins", "wellness", "white", "womens", "yogurt",
}
BRAND_FAMILY_ALIASES = {
    "Annie's": ["annie's homegrown", "annies homegrown", "annie's", "annies"],
    "Amylu": ["amylu foods", "amylu"],
    "Athletic Brewing": ["athletic brewing company", "athletic brewing"],
    "BelGioioso": ["belgioioso"],
    "Better Buzz Coffee": ["better buzz coffee"],
    "Brooklyn Brewery": ["brooklyn brewery"],
    "CREDO FOODS": ["credo foods", "credo"],
    "Garden of Life": ["garden of life"],
    "Health-Ade": ["health ade", "health-ade"],
    "MaryRuth's": ["maryruth's", "maryruths", "maryruth"],
    "Mrs. Meyer's": ["mrs. meyer's", "mrs meyers", "mrs. meyers"],
    "New Chapter": ["new chapter"],
    "OM Mushroom Superfood": ["om mushroom superfood"],
    "Siete": ["siete"],
    "Simply Organic": ["simply organic"],
    "Wellshire Farms": ["wellshire farms"],
    "Whole Foods Market": ["365 by whole foods market", "whole foods market"],
    "YumEarth": ["yumearth"],
}
BRAND_DESCRIPTOR_STARTERS = {
    "alfredo", "aged", "and", "bar", "bars", "bernie", "blanco", "bowl", "cheddar", "cheese", "chicken",
    "classic", "coffee", "cookies", "crackers", "deluxe", "frozen", "garlic", "growth", "homegrown", "item",
    "items", "liquid", "mac", "macaroni", "medium", "mineral", "mix", "multivitamin", "oatmilk", "organic",
    "pasta", "pepper", "pizza", "plant", "poppers", "powder", "pretzels", "protein", "queso", "real", "roasted",
    "sauce", "shells", "snack", "soup", "super", "supplement", "tomato", "uncured", "vitamin", "with",
}


def candidate_is_generic_brand(candidate):
    if not candidate:
        return True

    cleaned_tokens = [
        normalize_text_key(token)
        for token in re.split(r"\s+", candidate)
    ]
    cleaned_tokens = [
        token for token in cleaned_tokens
        if token and token not in BRAND_CONNECTORS
    ]
    if not cleaned_tokens:
        return True

    generic_hits = sum(1 for token in cleaned_tokens if token in GENERIC_BRAND_WORDS)
    if generic_hits == len(cleaned_tokens):
        return True
    if len(cleaned_tokens) >= 3 and generic_hits >= len(cleaned_tokens) - 1:
        return True
    return False


def canonical_brand_for_alias(candidate):
    normalized = normalize_text_key(candidate)
    if not normalized:
        return None

    for canonical, aliases in BRAND_FAMILY_ALIASES.items():
        for alias in aliases:
            alias_key = normalize_text_key(alias)
            if normalized == alias_key or normalized.startswith(alias_key + " "):
                return canonical
    return None


def trim_brand_candidate(candidate):
    if not candidate:
        return None

    canonical = canonical_brand_for_alias(candidate)
    if canonical:
        return canonical

    candidate = re.sub(r"^[®™©\s]+", "", candidate).strip(" ,:-")
    tokens = re.split(r"\s+", candidate)
    kept = []

    for index, token in enumerate(tokens):
        clean = token.strip(" ,.:;()[]{}")
        clean_key = normalize_text_key(clean)
        if not clean_key:
            continue
        if index > 0 and (clean_key in BRAND_DESCRIPTOR_STARTERS or any(ch.isdigit() for ch in clean_key)):
            break
        kept.append(clean)

    trimmed = " ".join(kept).strip(" ,:-") or candidate
    canonical = canonical_brand_for_alias(trimmed)
    if canonical:
        return canonical
    return clean_brand_display(trimmed)


def extract_brand_candidate(name):
    if not name:
        return None

    tokens = re.split(r"\s+", name.strip())
    brand_tokens = []

    def clean_token(token):
        return token.strip(" ,.:;()[]{}")

    def looks_brandish(token):
        cleaned = clean_token(token)
        if not cleaned:
            return False
        if cleaned.lower() in BRAND_STOP_WORDS:
            return False
        if any(ch.isdigit() for ch in cleaned):
            return False
        if cleaned.lower() in BRAND_CONNECTORS:
            return True
        return cleaned[0].isupper() or cleaned.isupper() or any(ch.isupper() for ch in cleaned[1:])

    for token in tokens[:5]:
        cleaned = clean_token(token)
        if not cleaned:
            continue
        lowered = cleaned.lower()

        if not brand_tokens:
            if looks_brandish(cleaned):
                brand_tokens.append(cleaned)
            else:
                break
            continue

        if lowered in BRAND_CONNECTORS or looks_brandish(cleaned):
            brand_tokens.append(cleaned)
            continue
        break

    if not brand_tokens:
        return None

    candidate = trim_brand_candidate(" ".join(brand_tokens).strip())
    if normalize_text_key(candidate) in BRAND_STOP_WORDS:
        return None
    if candidate_is_generic_brand(candidate):
        return None
    return candidate if len(candidate) >= 3 else None


def title_case_token(token, is_first=False):
    match = re.match(r"^([^A-Za-z0-9]*)([A-Za-z0-9’'&+/-]+)([^A-Za-z0-9]*)$", token)
    if not match:
        return token

    prefix, core, suffix = match.groups()
    lower_core = core.lower()

    if any(ch.isdigit() for ch in core):
        formatted = core.upper()
    elif core.isupper() and len(core) <= 4:
        formatted = core
    elif lower_core in SMALL_TITLE_WORDS and not is_first:
        formatted = lower_core
    else:
        pieces = re.split(r"([’'/-])", lower_core)
        rebuilt = []
        previous_separator = None
        for piece in pieces:
            if piece in {"’", "'", "/", "-"}:
                rebuilt.append(piece)
                previous_separator = piece
                continue
            if not piece:
                continue
            if previous_separator in {"’", "'"} and len(piece) == 1:
                rebuilt.append(piece.lower())
            else:
                rebuilt.append(piece.capitalize())
            previous_separator = None
        formatted = "".join(rebuilt)

    return f"{prefix}{formatted}{suffix}"


def smart_title_case(text):
    if not text:
        return text

    tokens = re.split(r"(\s+)", text.strip())
    output = []
    seen_word = False
    for token in tokens:
        if not token or token.isspace():
            output.append(token)
            continue
        output.append(title_case_token(token, is_first=not seen_word))
        seen_word = True
    return "".join(output)


def clean_brand_display(brand):
    if not brand:
        return brand
    brand = re.sub(r"\s+", " ", brand).strip(" ,")
    if brand.upper() == brand and len(brand.split()) >= 2:
        return smart_title_case(brand)
    return brand


def strip_brand_from_name(name, brand):
    if not name or not brand:
        return name

    if not name.lower().startswith(brand.lower()):
        return name

    remainder = name[len(brand):].lstrip(" ,:-–—")
    if not remainder:
        return name

    connector_pattern = r"^(?:" + "|".join(re.escape(word) for word in sorted(BRAND_CONNECTORS, key=len, reverse=True)) + r")\b[\s,:-]*"
    while True:
        updated = re.sub(connector_pattern, "", remainder, flags=re.IGNORECASE)
        if updated == remainder:
            break
        remainder = updated.lstrip(" ,:-–—")
        if not remainder:
            return name

    normalized_remainder = normalize_text_key(remainder)
    if normalized_remainder in GENERIC_REMAINDER_NAMES:
        return name

    remainder_word_count = len(normalized_remainder.split())
    if remainder_word_count < 2 and len(remainder) < 14:
        return name

    return remainder


def clean_display_name(name, brand=None):
    if not name:
        return name

    cleaned = re.sub(r"\s+", " ", str(name)).strip(" ,")
    cleaned = re.sub(r"(?<=\w)\+(?=\w)", " + ", cleaned)
    cleaned = strip_brand_from_name(cleaned, brand)
    cleaned = re.sub(r"^[®™©\s]+", "", cleaned)
    cleaned = re.sub(r"^homegrown[\s,:-]+", "", cleaned, flags=re.IGNORECASE)

    if "|" in cleaned:
        cleaned = cleaned.split("|", 1)[0].strip(" ,")

    if " – " in cleaned and (len(cleaned) > 55 or any(marker in cleaned.lower() for marker in DISPLAY_NAME_CLAIM_MARKERS)):
        cleaned = cleaned.split(" – ", 1)[0].strip(" ,")
    elif " - " in cleaned and (len(cleaned) > 55 or any(marker in cleaned.lower() for marker in DISPLAY_NAME_CLAIM_MARKERS)):
        cleaned = cleaned.split(" - ", 1)[0].strip(" ,")

    comma_parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    if len(comma_parts) > 1:
        trailing = ", ".join(comma_parts[1:]).lower()
        if len(cleaned) > 60 or any(marker in trailing for marker in DISPLAY_NAME_CLAIM_MARKERS):
            cleaned = comma_parts[0]

    cleaned = re.sub(r"\s*\((?:\d+\s*servings?|pack of \d+|[0-9.]+\s*(?:oz|fl oz|lb|g|kg).*)\)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;-–—")
    return smart_title_case(cleaned)


def normalize_brands_across_products(products):
    for product in products:
        canonical_brand = trim_brand_candidate(product.get("brand")) if product.get("brand") else None
        if canonical_brand and candidate_is_generic_brand(canonical_brand):
            canonical_brand = None

        product["brand"] = canonical_brand
        source_name = product.get("raw_name") or product.get("name")
        if source_name:
            product["name"] = clean_display_name(source_name, canonical_brand)
        product["tags"] = derive_tags(
            name=product.get("name"),
            brand=product.get("brand"),
            category=product.get("category"),
            sources=product.get("sources"),
            source_count=product.get("source_count", 0),
            prime_price=product.get("prime_price"),
        )
    return products


def build_classification_haystack(name=None, brand=None, variation=None, url=None):
    haystack = normalize_text_key(" ".join(filter(None, [name, brand, variation, url])))
    if not haystack:
        return ""

    haystack = haystack.replace("fl oz", " fluid ounce")
    haystack = haystack.replace("oz cans", " cans")
    haystack = haystack.replace("sparkling water beverage", "sparkling water")
    haystack = haystack.replace("fruit spread preserves", "fruit spread")
    return haystack


def score_category_profile(haystack, profile):
    score = 0
    reasons = []

    for phrase in profile.get("strong", []):
        if text_contains_phrase(haystack, phrase):
            score += 12
            reasons.append(phrase)
    for phrase in profile.get("medium", []):
        if text_contains_phrase(haystack, phrase):
            score += 6
            reasons.append(phrase)
    for phrase in profile.get("weak", []):
        if text_contains_phrase(haystack, phrase):
            score += 2
            reasons.append(phrase)
    for phrase in profile.get("exclude", []):
        if text_contains_phrase(haystack, phrase):
            score -= 10

    return score, reasons


def derive_subcategory(category, haystack):
    subcategories = SUBCATEGORY_PROFILES.get(category, {})
    best_subcategory = None
    best_score = 0

    for subcategory, phrases in subcategories.items():
        score = 0
        for phrase in phrases:
            if text_contains_phrase(haystack, phrase):
                score += 4 if " " in normalize_text_key(phrase) else 2
        if score > best_score:
            best_score = score
            best_subcategory = subcategory

    return best_subcategory


def derive_category_details(name, brand=None, variation=None, url=None):
    haystack = build_classification_haystack(name=name, brand=brand, variation=variation, url=url)
    if not haystack:
        return {
            "category": "Pantry",
            "subcategory": None,
            "confidence": 0.2,
            "signals": [],
        }

    for hint in DIRECT_CATEGORY_HINTS:
        if any(text_contains_phrase(haystack, phrase) for phrase in hint["include"]) and not any(
            text_contains_phrase(haystack, phrase) for phrase in hint.get("exclude", [])
        ):
            category = hint["category"]
            return {
                "category": category,
                "subcategory": derive_subcategory(category, haystack),
                "confidence": 0.93,
                "signals": [f"direct match: {hint['include'][0]}"],
            }

    best_category = "Pantry"
    best_score = -999
    second_best_score = -999
    best_reasons = []

    for category, profile in CATEGORY_PROFILES.items():
        score, reasons = score_category_profile(haystack, profile)
        if score > best_score:
            second_best_score = best_score
            best_score = score
            best_category = category
            best_reasons = reasons
        elif score > second_best_score:
            second_best_score = score

    if best_score <= 0:
        return {
            "category": "Pantry",
            "subcategory": derive_subcategory("Pantry", haystack),
            "confidence": 0.25,
            "signals": [],
        }

    margin = max(0, best_score - second_best_score)
    confidence = min(0.99, max(0.3, 0.35 + (best_score / 40.0) + (margin / 30.0)))
    return {
        "category": best_category,
        "subcategory": derive_subcategory(best_category, haystack),
        "confidence": round(confidence, 2),
        "signals": best_reasons[:6],
    }


def derive_category(name, brand=None, variation=None, url=None):
    return derive_category_details(name, brand=brand, variation=variation, url=url)["category"]


def derive_tags(name, brand=None, category=None, sources=None, source_count=0, prime_price=None):
    haystack = " ".join(filter(None, [name, brand, category])).lower()
    tags = []

    for tag, keywords in TAG_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            tags.append(tag)

    sources = sources or []
    if source_count > 1 and "multi-source deal" not in tags:
        tags.append("multi-source deal")
    elif len(sources) == 1 and sources[0] == "Flyer":
        tags.append("flyer-only deal")

    if prime_price:
        tags.append("prime deal")

    return sorted(dict.fromkeys(tags))


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


def replace_price_suffix(price_str, suffix):
    if not price_str:
        return price_str

    s = price_str.strip()
    if is_non_price_promo_text(s):
        return clean_percent_text(s) if is_percent_off_text(s) else s
    if "prices vary" in s.lower():
        return clean_regular_price_text(s)

    match = re.match(r'^(\$\d+(?:\.\d{1,2})?(?:\s+to\s+\$\d+(?:\.\d{1,2})?)?)(.*)$', s)
    if not match:
        return s

    return match.group(1) + suffix


def harmonize_prime_suffix(display_regular_price, display_prime_price):
    if not display_regular_price or not display_prime_price:
        return display_prime_price
    return replace_price_suffix(
        ensure_price_has_suffix(display_prime_price, display_regular_price),
        infer_suffix_from_regular_price(display_regular_price),
    )


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
            display_prime_price = harmonize_prime_suffix(display_regular_price, display_prime_price)
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
            display_prime_price = harmonize_prime_suffix(display_regular_price, display_prime_price)
            if normalize_text_key(display_regular_price) == normalize_text_key(display_prime_price):
                display_regular_price = None

        return display_regular_price, display_prime_price, final_discount

    if candidate_regular:
        return add_ea_if_needed(candidate_regular), None, strongest_promo or candidate_discount

    if candidate_prime:
        if is_percent_off_text(candidate_prime):
            return None, None, strongest_promo or clean_percent_text(candidate_prime)
        return None, ensure_price_has_suffix(candidate_prime, regular_price or current_price), strongest_promo or candidate_discount

    return None, None, strongest_promo or candidate_discount


def standardize_product_record(
    *,
    name,
    raw_name=None,
    image=None,
    url=None,
    asin=None,
    asins=None,
    brand=None,
    variation=None,
    regular_price=None,
    prime_price=None,
    current_price=None,
    discount_text=None,
    unit_price=None,
    emoji=None,
    classification_context=None,
    extra_fields=None,
):
    source_name = raw_name or name
    display_regular_price, display_prime_price, final_discount = resolve_display_pricing(
        regular_price=regular_price,
        prime_price=prime_price,
        current_price=current_price,
        discount_text=discount_text,
    )
    normalized_brand = derive_brand(source_name, explicit_brand=brand)
    classification_text = " ".join(
        part
        for part in [
            variation,
            " ".join(classification_context or []),
        ]
        if part
    )
    category_details = derive_category_details(
        source_name,
        brand=normalized_brand,
        variation=classification_text or variation,
        url=url,
    )
    category = category_details["category"]
    discount_percent = extract_discount_sort_value(final_discount)
    display_name = clean_display_name(source_name, normalized_brand)

    product = {
        "asin": asin,
        "name": display_name,
        "raw_name": source_name,
        "brand": normalized_brand,
        "variation": variation,
        "category": category,
        "subcategory": category_details.get("subcategory"),
        "category_confidence": category_details.get("confidence"),
        "category_signals": category_details.get("signals", []),
        "image": image,
        "url": url,
        "unit_price": unit_price,
        "current_price": current_price,
        "basis_price": display_regular_price,
        "prime_price": display_prime_price,
        "discount": final_discount,
        "discount_percent": discount_percent if discount_percent >= 0 else 0,
        "emoji": emoji or emoji_for_product(name),
        "available_store_ids": list(DEFAULT_STORE_IDS),
    }

    if asins:
        product["asins"] = asins
        if not product.get("asin"):
            product["asin"] = str(asins[0]).strip()

    product["tags"] = derive_tags(
        name=source_name,
        brand=normalized_brand,
        category=category,
        prime_price=display_prime_price,
    )

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
                raw_name=p.get("raw_name"),
                brand=p.get("brand"),
                variation=p.get("variation"),
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
                raw_name=p.get("raw_name"),
                brand=p.get("brand"),
                variation=p.get("variation"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                emoji=p.get("emoji"),
                extra_fields={"retailer": p.get("retailer") or "Whole Foods"},
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
                raw_name=p.get("raw_name"),
                brand=p.get("brand"),
                variation=p.get("variation"),
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


def load_target_deals():
    print("Looking for target deals file at:", TARGET_DEALS_FILE)

    try:
        with open(TARGET_DEALS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("Target deals file not found.")
        return []

    print("Loaded", len(raw_products), "target deals")

    products = []
    for p in raw_products:
        products.append(
            standardize_product_record(
                asin=p.get("asin"),
                name=p.get("name"),
                raw_name=p.get("raw_name"),
                brand=p.get("brand"),
                variation=p.get("variation"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                extra_fields={
                    "retailer": "Target",
                    "expires": p.get("expires"),
                },
            )
        )

    return products


def load_hmart_deals():
    print("Looking for H Mart deals file at:", HMART_DEALS_FILE)

    try:
        with open(HMART_DEALS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("H Mart deals file not found.")
        return []

    print("Loaded", len(raw_products), "H Mart deals")

    products = []
    for p in raw_products:
        products.append(
            standardize_product_record(
                asin=p.get("asin"),
                name=p.get("name"),
                raw_name=p.get("raw_name"),
                brand=p.get("brand"),
                variation=p.get("variation"),
                image=p.get("image"),
                url=p.get("url"),
                unit_price=p.get("unit_price"),
                current_price=p.get("current_price"),
                regular_price=p.get("basis_price"),
                prime_price=p.get("prime_price"),
                discount_text=p.get("discount"),
                extra_fields={
                    "retailer": "H Mart",
                    "retail_source_url": p.get("retail_source_url"),
                    "source_categories": p.get("categories") or [],
                },
                classification_context=p.get("categories") or [],
            )
        )

    return products


def merge_combined_product(existing, incoming):
    if not existing:
        merged = dict(incoming)
        merged["sources"] = list(incoming.get("sources", []))
        merged["source_count"] = len(merged["sources"])
        merged["tags"] = derive_tags(
            name=merged.get("name"),
            brand=merged.get("brand"),
            category=merged.get("category"),
            sources=merged.get("sources"),
            source_count=merged.get("source_count", 0),
            prime_price=merged.get("prime_price"),
        )
        return merged

    merged = dict(existing)

    for key, value in incoming.items():
        if key in {"sources", "tags", "available_store_ids"}:
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
    merged_store_ids = []
    for store_id in existing.get("available_store_ids", []) + incoming.get("available_store_ids", []):
        if store_id not in merged_store_ids:
            merged_store_ids.append(store_id)
    merged["available_store_ids"] = merged_store_ids or list(DEFAULT_STORE_IDS)
    if merged.get("basis_price") and merged.get("prime_price"):
        merged["prime_price"] = harmonize_prime_suffix(merged.get("basis_price"), merged.get("prime_price"))
    merged["tags"] = derive_tags(
        name=merged.get("name"),
        brand=merged.get("brand"),
        category=merged.get("category"),
        sources=merged_sources,
        source_count=len(merged_sources),
        prime_price=merged.get("prime_price"),
    )
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
        raw_name=product.get("raw_name"),
        brand=product.get("brand"),
        variation=product.get("variation"),
        image=product.get("image"),
        url=product.get("url"),
        unit_price=product.get("unit_price"),
        current_price=product.get("current_price") or product.get("sale_price"),
        regular_price=product.get("basis_price"),
        prime_price=product.get("prime_price"),
        discount_text=product.get("discount"),
        emoji=product.get("emoji"),
        classification_context=product.get("source_categories") or [],
        extra_fields={
            "retailer": product.get("retailer"),
            "expires": product.get("expires"),
            "retail_source_url": product.get("retail_source_url"),
            "source_categories": product.get("source_categories") or [],
        },
    )
    normalized["sources"] = [source_name]
    normalized["tags"] = derive_tags(
        name=normalized.get("name"),
        brand=normalized.get("brand"),
        category=normalized.get("category"),
        sources=normalized.get("sources"),
        source_count=1,
        prime_price=normalized.get("prime_price"),
    )
    return normalized


def build_combined_products(
    flyer_products,
    all_deals_products,
    search_deals_products,
    target_deals_products=None,
    hmart_deals_products=None,
):
    combined = {}
    target_deals_products = target_deals_products or []
    hmart_deals_products = hmart_deals_products or []

    datasets = [
        ("Flyer", flyer_products),
        ("All Deals", all_deals_products),
        ("Search Deals", search_deals_products),
        ("Target Deals", target_deals_products),
        ("H Mart Deals", hmart_deals_products),
    ]

    for source_name, products in datasets:
        for product in products:
            normalized = normalized_product_for_source(product, source_name)
            key = combined_key_for_product(normalized)
            combined[key] = merge_combined_product(combined.get(key), normalized)

    ordered = list(combined.values())
    ordered = normalize_brands_across_products(ordered)
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
        target_deals_products = load_target_deals()
        hmart_deals_products = load_hmart_deals()
        return build_combined_products(
            flyer_products,
            all_deals_products,
            search_deals_products,
            target_deals_products,
            hmart_deals_products,
        )

    print("Loaded", len(products), "combined products")
    normalized_products = []
    for p in products:
        normalized = standardize_product_record(
            asin=p.get("asin"),
            asins=p.get("asins"),
            name=p.get("name"),
            raw_name=p.get("raw_name"),
            brand=p.get("brand"),
            variation=p.get("variation"),
            image=p.get("image"),
            url=p.get("url"),
            unit_price=p.get("unit_price"),
            current_price=p.get("current_price") or p.get("sale_price"),
            regular_price=p.get("basis_price"),
            prime_price=p.get("prime_price"),
            discount_text=p.get("discount"),
            emoji=p.get("emoji"),
            classification_context=p.get("source_categories") or [],
            extra_fields={
                "retailer": p.get("retailer"),
                "expires": p.get("expires"),
                "retail_source_url": p.get("retail_source_url"),
                "source_categories": p.get("source_categories") or [],
            },
        )
        sources = list(p.get("sources") or [])
        normalized["sources"] = sources
        normalized["source_count"] = len(sources)
        normalized["tags"] = derive_tags(
            name=normalized.get("name"),
            brand=normalized.get("brand"),
            category=normalized.get("category"),
            sources=sources,
            source_count=len(sources),
            prime_price=normalized.get("prime_price"),
        )
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
                brand=p.get("brandName"),
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


def parse_csv_arg(name):
    raw_value = request.args.get(name, "")
    values = [value.strip() for value in raw_value.split(",") if value.strip()]
    return values


def filter_products_for_api(products):
    query = normalize_text_key(request.args.get("q", ""))
    categories = {value.lower() for value in parse_csv_arg("category")}
    tags = {value.lower() for value in parse_csv_arg("tag")}
    brands = {value.lower() for value in parse_csv_arg("brand")}
    retailers = {value.lower() for value in parse_csv_arg("retailer")}
    store_ids = set(parse_csv_arg("store_id"))

    filtered = []
    for product in products:
        haystack = normalize_text_key(
            " ".join(
                filter(
                    None,
                    [
                        product.get("name"),
                        product.get("brand"),
                        product.get("category"),
                        product.get("asin"),
                        " ".join(product.get("tags") or []),
                    ],
                )
            )
        )

        if query and query not in haystack:
            continue
        if categories and (product.get("category") or "").lower() not in categories:
            continue
        if tags and not tags.intersection({tag.lower() for tag in product.get("tags") or []}):
            continue
        if brands and (product.get("brand") or "").lower() not in brands:
            continue
        if retailers and (product.get("retailer") or "").lower() not in retailers:
            continue
        if store_ids:
            available_store_ids = set(product.get("available_store_ids") or [])
            if available_store_ids and not store_ids.intersection(available_store_ids):
                continue

        filtered.append(product)

    return filtered


def api_limit(default=60, maximum=200):
    try:
        value = int(request.args.get("limit", default))
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, maximum))


@app.route("/service-worker.js")
def service_worker():
    return send_from_directory(os.path.join(BASE_DIR, "static"), "service-worker.js", mimetype="application/javascript")


@app.route("/manifest.webmanifest")
def manifest():
    return send_from_directory(os.path.join(BASE_DIR, "static"), "manifest.webmanifest", mimetype="application/manifest+json")


@app.route("/api/stores")
def api_stores():
    return jsonify({"stores": SUPPORTED_STORES})


@app.route("/api/categories")
def api_categories():
    products = load_combined_products()
    counts = {}
    for product in products:
        category = product.get("category") or "Pantry"
        counts[category] = counts.get(category, 0) + 1

    categories = [
        {"name": category, "count": count}
        for category, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return jsonify({"categories": categories})


@app.route("/api/search")
def api_search():
    products = sort_products_for_display(filter_products_for_api(load_combined_products()))
    return jsonify({"products": products[:api_limit()], "count": len(products)})


@app.route("/api/feed")
def api_feed():
    products = sort_products_for_display(filter_products_for_api(load_combined_products()))
    return jsonify({"products": products[:api_limit()], "count": len(products)})


@app.route("/api/product/<asin>")
def api_product(asin):
    asin = asin.strip()
    for product in load_combined_products():
        asins = set(product.get("asins") or [])
        if product.get("asin") == asin or asin in asins:
            return jsonify(product)
    return jsonify({"error": "Product not found"}), 404


@app.route("/")
def combined_products_home():
    products = sort_products_for_display(load_combined_products())
    deal_count = len(products)
    return render_template(
        "combined_products.html",
        products=products,
        deal_count=deal_count,
        available_stores=SUPPORTED_STORES,
        page_subtitle="Browse Whole Foods, Target, and H Mart deals in one place.",
    )


if __name__ == "__main__":
    app.run(debug=True)
