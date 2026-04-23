import json
import re
import requests
import os
import math
from collections import Counter, defaultdict
from functools import lru_cache
from urllib.parse import urljoin
from flask import Flask, jsonify, render_template, request, send_from_directory
from brand_ai import build_brand_family_map
from taxonomy_ai import (
    CLASSIFICATION_CACHE_FILE as TAXONOMY_CLASSIFICATION_CACHE_FILENAME,
    DISCOVERED_TAXONOMY_FILE as DISCOVERED_TAXONOMY_FILENAME,
    FAILED_CATEGORY,
    MODEL_VERSION as TAXONOMY_AI_MODEL_VERSION,
    OLLAMA_MODEL as TAXONOMY_AI_MODEL_NAME,
    PROMPT_VERSION as TAXONOMY_AI_PROMPT_VERSION,
    apply_failed_classification_bucket,
    build_cache_artifact,
    build_report_artifact,
    build_taxonomy_artifact,
    classify_products as classify_products_with_taxonomy_ai,
    ensure_taxonomy,
    taxonomy_to_options,
)
from subcategory_ai import (
    MODEL_VERSION as SUBCATEGORY_AI_MODEL_VERSION,
    build_change_report as build_subcategory_ai_change_report,
    load_model_artifacts as load_subcategory_ai_artifacts,
    predict_subcategories,
    save_model_artifacts as save_subcategory_ai_artifacts,
    train_subcategory_model,
)
from supabase_state import (
    load_device_profile_from_supabase,
    load_fixes_from_supabase,
    save_device_profile_to_supabase,
    save_fix_to_supabase,
    supabase_enabled,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DISCOVERED_DEALS_FILE = os.path.join(BASE_DIR, "discovered_products.json")
SEARCH_DEALS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")
FLYER_PRODUCTS_FILE = os.path.join(BASE_DIR, "flyer_products.json")
COMBINED_PRODUCTS_FILE = os.path.join(BASE_DIR, "combined_products.json")
TARGET_DEALS_FILE = os.path.join(BASE_DIR, "target_deals_products.json")
HMART_DEALS_FILE = os.path.join(BASE_DIR, "hmart_deals_products.json")
FIXES_TO_DEPLOY_FILE = os.path.join(BASE_DIR, "fixes_to_deploy.json")
DEVICE_PROFILES_FILE = os.path.join(BASE_DIR, "device_profiles.json")
TAXONOMY_GOLD_LABELS_FILE = os.path.join(BASE_DIR, "taxonomy_gold_labels.json")
SUBCATEGORY_AI_MODEL_FILE = os.path.join(BASE_DIR, "subcategory_ai_model.pkl")
SUBCATEGORY_AI_METADATA_FILE = os.path.join(BASE_DIR, "subcategory_ai_metadata.json")
SUBCATEGORY_AI_REPORT_FILE = os.path.join(BASE_DIR, "subcategory_ai_report.json")
DISCOVERED_TAXONOMY_FILE = build_taxonomy_artifact(BASE_DIR)
TAXONOMY_CLASSIFICATION_CACHE_FILE = build_cache_artifact(BASE_DIR)
TAXONOMY_AI_REPORT_FILE = build_report_artifact(BASE_DIR)
SUBCATEGORY_AI_MIN_CONFIDENCE = 0.0

app = Flask(__name__)
PUBLIC_API_BASE_URL = os.getenv("PUBLIC_API_BASE_URL", "").rstrip("/")
CORS_ALLOW_ORIGIN = os.getenv("CORS_ALLOW_ORIGIN", "*")
API_ONLY_MODE = os.getenv("API_ONLY_MODE", "").strip().lower() in {"1", "true", "yes"}


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = CORS_ALLOW_ORIGIN
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response

WHOLE_FOODS_RETAILER = "Whole Foods"
TARGET_RETAILER = "Target"
HMART_RETAILER = "H Mart"

WHOLE_FOODS_STORE_CONFIG = [
    {
        "id": "10160",
        "slug": "columbus-circle",
        "name": "Columbus Circle",
        "city": "New York",
        "state": "NY",
        "label": "Columbus Circle, NYC",
        "address": "10 Columbus Cir Ste SC101, New York, NY 10019",
        "is_active": True,
    },
    {
        "id": "bryant-park",
        "slug": "bryant-park",
        "name": "Bryant Park",
        "city": "New York",
        "state": "NY",
        "label": "Bryant Park, NYC",
        "address": "1095 6th Ave, New York, NY 10036",
        "is_active": False,
        "needs_store_id": True,
    },
    {
        "id": "manhattan-west",
        "slug": "manhattan-west",
        "name": "Manhattan West",
        "city": "New York",
        "state": "NY",
        "label": "Manhattan West, NYC",
        "address": "450 W 33rd St, New York, NY 10001",
        "is_active": False,
        "needs_store_id": True,
    },
    {
        "id": "10328",
        "slug": "upper-west-side",
        "name": "Upper West Side",
        "city": "New York",
        "state": "NY",
        "label": "Upper West Side, NYC",
        "address": "808 Columbus Ave, New York, NY 10025",
        "is_active": True,
    },
    {
        "id": "union-square",
        "slug": "union-square",
        "name": "Union Square",
        "city": "New York",
        "state": "NY",
        "label": "Union Square, NYC",
        "address": "4 Union Square S, New York, NY 10003",
        "is_active": False,
        "needs_store_id": True,
    },
]


def load_supported_stores():
    configured = os.getenv("WHOLEFOODS_STORES_JSON", "").strip()
    if configured:
        try:
            stores = json.loads(configured)
            if isinstance(stores, list) and stores:
                return stores
        except json.JSONDecodeError:
            print("WHOLEFOODS_STORES_JSON was not valid JSON; using built-in store config.")
    return [dict(store) for store in WHOLE_FOODS_STORE_CONFIG]


SUPPORTED_STORES = load_supported_stores()
ACTIVE_WHOLE_FOODS_STORES = [store for store in SUPPORTED_STORES if store.get("is_active")]
ACTIVE_WHOLE_FOODS_STORE_IDS = [str(store["id"]) for store in ACTIVE_WHOLE_FOODS_STORES if store.get("id")]
DEFAULT_STORE_IDS = [ACTIVE_WHOLE_FOODS_STORES[0]["id"] if ACTIVE_WHOLE_FOODS_STORES else SUPPORTED_STORES[0]["id"]]
SALES_FLYER_URL = f"https://www.wholefoodsmarket.com/sales-flyer?store-id={DEFAULT_STORE_IDS[0]}"


def build_sales_flyer_url(store_id=None):
    selected_store_id = str(store_id or DEFAULT_STORE_IDS[0])
    return f"https://www.wholefoodsmarket.com/sales-flyer?store-id={selected_store_id}"
CATEGORY_PROFILES = {
    "Produce": {
        "strong": [
            "fresh fruit", "fresh vegetable", "salad kit", "salad mix", "baby spinach",
            "romaine", "broccoli florets", "cauliflower florets", "avocado", "apple",
            "banana", "grapes", "berries", "blueberries", "strawberries", "raspberries",
            "blackberries", "cherries", "fresh cherries", "citrus", "lettuce", "tomato",
            "onion", "carrot", "mango", "kiwi", "pear", "peach", "plum", "melon",
            "pineapple", "fresh mushroom", "mushroom", "mushrooms", "asparagus",
            "cilantro", "parsley", "basil", "spring mix", "greens",
        ],
        "medium": ["produce", "fruit", "vegetable", "salad", "greens", "fresh cut"],
        "weak": ["cherry", "lemon", "orange", "lime"],
        "exclude": [
            "cherry cola", "cherry gummies", "gummy", "candy", "chocolate", "cookie",
            "granola bar", "protein bar", "sparkling", "seltzer", "juice box",
            "mushroom coffee", "mushroom powder", "mushroom supplement", "broth",
            "pasta", "sauce", "soup", "canned", "meal", "cheese", "wine", "beer",
            "hard seltzer", "adaptogen", "reishi", "cordyceps", "propolis",
            "throat spray", "syrup", "elderberry", "goldenseal", "echinacea",
            "immune support", "throat health", "sparkling adaptogen", "drink",
        ],
    },
    "Meat & Seafood": {
        "strong": [
            "chicken", "beef", "steak", "ground beef", "salmon", "tuna", "fish",
            "shrimp", "bacon", "turkey", "ham", "lamb", "sausage", "pork",
            "scallop", "seafood", "crab", "lobster", "meatballs", "cutlet",
            "charcuterie", "prosciutto", "deli meat",
        ],
        "medium": ["meat", "seafood", "poultry", "jerky", "charcuterie"],
        "weak": [],
        "exclude": [
            "dog food", "chips", "chip", "cracker", "crackers", "pretzel", "pretzels",
            "popcorn", "crisps", "dip", "dipping sauce", "pasta sauce", "bolognese sauce",
            "queso", "dressing", "marinade", "alfredo", "sauce dip", "protein chips",
            "broth", "soup", "kimchi", "tea", "wine",
        ],
    },
    "Dairy & Eggs": {
        "strong": [
            "milk", "cheese", "butter", "yogurt", "egg", "cream cheese", "kefir",
            "cottage cheese", "sour cream", "half and half", "cream", "mozzarella",
            "cheddar", "feta", "parmesan", "cultured dairy", "dairy spread",
        ],
        "medium": ["dairy", "creamer", "buttermilk"],
        "weak": [],
        "exclude": [
            "ice cream", "frozen dessert", "seed butter", "sunflower butter",
            "almond butter", "cashew butter", "nut butter", "peanut butter",
            "chocolate egg", "chocolate eggs", "queso", "alfredo", "macaroni",
            "macaroni and cheese", "mac cheese", "shells and cheddar", "tea",
            "herbal tea", "milk thistle", "soup", "broth", "liquid extract",
            "herbal supplement", "liver support", "protein shake", "oatmilk",
        ],
    },
    "Bakery": {
        "strong": [
            "bread", "croissant", "cake", "muffin", "bagel", "pastry", "brownie",
            "donut", "tortilla", "bun", "roll", "scone", "danish", "quiche",
            "fresh baked", "bakery",
        ],
        "medium": ["bakery", "baked", "pastry"],
        "weak": [],
        "exclude": [
            "ice cream cake", "pancake mix", "waffle mix", "chips", "chip", "tortilla chips",
            "rolled tortilla chips", "pretzel", "pretzels", "popcorn", "crisps", "puffs",
            "chickpea puffs", "cracker", "crackers", "wafel", "wafels", "wafer", "wafers",
            "stroopwafel", "pie filling",
        ],
    },
    "Prepared Foods": {
        "strong": [
            "kimchi", "kimbap", "gimbap", "banchan", "side dish", "sidedish", "deli",
            "dumpling", "mandu", "tteokbokki", "katsu", "ready meal", "prepared meal",
            "instant food", "rice bowl", "fried rice", "sushi", "meal kit",
            "prepared protein", "deli salad", "quick meal", "soup dumpling",
        ],
        "medium": ["prepared", "quick food", "heat and eat", "ready to eat", "deli"],
        "weak": [],
        "exclude": ["dish soap", "laundry", "pet food", "broth concentrate"],
    },
    "Frozen": {
        "strong": [
            "frozen", "ice cream", "gelato", "pizza", "waffle", "popsicle", "sorbet",
            "frozen dessert", "ice pop", "frozen fruit", "frozen vegetable",
            "ice cream sandwich", "breakfast burrito",
        ],
        "medium": ["freezer", "frozen"],
        "weak": [],
        "exclude": [],
    },
    "Snacks": {
        "strong": [
            "chips", "cracker", "pretzel", "popcorn", "snack", "granola bar", "candy",
            "chocolate", "bites", "crisps", "gummy", "trail mix", "snack bar",
            "protein bar", "fruit snacks", "cookies", "cookie", "biscuit", "biscuits",
            "wafel", "wafels", "wafer", "wafers", "stroopwafel", "coconut chips",
            "chocolate egg", "chocolate eggs", "puffs", "jerky", "bars",
        ],
        "medium": ["bar", "jerky", "nuts", "chews", "trail mix", "popcorn"],
        "weak": ["cherry", "berry"],
        "exclude": ["broth", "protein powder", "dish soap", "bread", "bagel", "wine"],
    },
    "Pantry": {
        "strong": [
            "pasta", "rice", "sauce", "vinegar", "oil", "flour", "spice", "seasoning",
            "broth", "beans", "soup", "hummus", "bruschetta", "oatmeal", "cereal",
            "granola", "peanut butter", "jam", "honey", "mustard", "ketchup", "marinade",
            "dressing", "salsa", "fruit spread", "spread", "preserves", "seed butter",
            "sunflower butter", "almond butter", "cashew butter", "nut butter", "hommus",
            "queso", "alfredo", "macaroni", "macaroni and cheese", "mac cheese",
            "shells and cheddar", "dip", "dipping sauce", "pasta sauce", "bolognese",
            "bolognese sauce", "avocado oil", "olive oil", "mayo", "mayonnaise",
            "vinaigrette", "aioli", "oats", "overnight oats", "lentils", "legumes",
            "baking mix", "meal kit", "stock", "condiment", "chili crisp",
            "pie filling", "tomato paste", "pesto",
        ],
        "medium": ["pantry", "mix", "canned", "jarred", "breakfast pantry"],
        "weak": [],
        "exclude": ["cake", "cookie", "chips", "sparkling water", "beer", "wine"],
    },
    "Beverages": {
        "strong": [
            "coffee", "tea", "juice", "water", "seltzer", "soda", "kombucha", "smoothie",
            "latte", "drink", "cold brew", "sparkling water", "energy drink",
            "coconut water", "zero proof", "non alcoholic", "non-alcoholic",
            "cream soda", "electrolyte drink", "drink mix", "hydration",
            "sparkling adaptogen", "adaptogen drink", "fluid ounce bottle",
        ],
        "medium": ["beverage", "mocktail", "sports drink"],
        "weak": [],
        "exclude": [
            "drinkware", "beer", "wine", "hard seltzer", "cider", "spirits",
            "vodka", "whiskey", "whisky", "tequila", "rum", "gin", "bourbon",
        ],
    },
    "Alcohol": {
        "strong": [
            "beer", "wine", "hard seltzer", "cider", "vodka", "whiskey", "whisky",
            "tequila", "rum", "gin", "bourbon", "cabernet", "merlot", "chardonnay",
            "riesling", "pinot", "sauvignon", "prosecco", "rose", "rosé", "lager",
            "ipa", "stout", "pilsner", "ale", "spritz", "spiked", "cocktail",
            "martini", "mezcal", "sake", "soju",
        ],
        "medium": ["alcohol", "spirits", "canned cocktail", "beer", "wine"],
        "weak": [],
        "exclude": ["wine vinegar", "mocktail", "zero proof", "non alcoholic", "non-alcoholic"],
    },
    "Supplements & Wellness": {
        "strong": [
            "vitamin", "supplement", "enzyme", "probiotic", "collagen", "magnesium",
            "omega", "capsule", "wellness", "multivitamin", "shots", "shot", "peptides",
            "digestive", "powder", "electrolyte", "turmeric", "elixir", "calcium",
            "sleep support", "stress support", "hair growth", "herbal remedy",
            "functional mushroom", "propolis", "throat spray", "echinacea",
            "goldenseal", "elderberry syrup", "licorice", "immune support",
            "throat health", "sambucus",
        ],
        "medium": ["protein", "greens powder", "wellness", "tonic", "adaptogen", "mushroom blend"],
        "weak": [],
        "exclude": ["protein bar", "shot glass", "energy drink", "coffee creamer"],
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
            "lip balm", "sunscreen", "for face", "for body", "face and body",
            "skin care", "oral care", "baby lotion", "baby wash",
        ],
        "medium": ["beauty", "personal care", "soap"],
        "weak": [],
        "exclude": ["dish soap", "laundry soap"],
    },
}
SUBCATEGORY_PROFILES = {
    "Produce": {
        "Fruit": ["apple", "banana", "berries", "berry", "grapes", "grape", "cherries", "cherry", "citrus", "orange", "lemon", "lime", "kiwi", "mango", "pear", "peach", "plum", "melon", "pineapple", "strawberries", "blueberries", "raspberries", "blackberries"],
        "Vegetables": ["broccoli", "cauliflower", "lettuce", "tomato", "onion", "carrot", "pepper", "cucumber", "avocado", "potato", "sweet potato", "kale", "spinach", "asparagus"],
        "Salads & Greens": ["salad", "greens", "romaine", "baby spinach", "spring mix", "salad kit", "salad mix", "arugula"],
        "Herbs": ["cilantro", "parsley", "basil", "mint", "fresh herbs"],
        "Mushrooms": ["mushroom", "mushrooms", "shiitake", "oyster mushroom", "portobello", "cremini"],
        "Fresh Cut & Prepared Produce": ["fresh cut", "cut fruit", "fruit cup", "prepared produce"],
    },
    "Meat & Seafood": {
        "Chicken & Turkey": ["chicken", "turkey", "cutlet", "breast", "thigh", "drumstick"],
        "Beef, Pork & Lamb": ["beef", "pork", "lamb", "ham", "steak", "ground beef"],
        "Seafood": ["salmon", "tuna", "shrimp", "fish", "seafood", "crab", "lobster", "scallop"],
        "Sausages & Meatballs": ["sausage", "sausages", "meatballs", "meatball"],
        "Deli Meat & Charcuterie": ["deli meat", "prosciutto", "charcuterie", "pepperoni", "salami"],
    },
    "Dairy & Eggs": {
        "Milk & Creamers": ["milk", "creamer", "half and half", "kefir"],
        "Cheese": ["cheese", "mozzarella", "cheddar", "feta", "parmesan", "cream cheese", "cottage cheese"],
        "Yogurt & Cultured Dairy": ["yogurt", "kefir", "cultured"],
        "Eggs & Butter": ["egg", "butter", "sour cream"],
        "Dips & Dairy Spreads": ["pimento cheese", "labneh", "dairy spread", "cheese dip"],
    },
    "Bakery": {
        "Bread & Bagels": ["bread", "bagel", "bun", "roll"],
        "Pastries & Desserts": ["croissant", "cake", "muffin", "pie", "pastry", "brownie", "donut", "scone"],
        "Cookies & Biscuits": ["bakery cookie", "fresh baked cookie", "butter cookie tin", "biscotti", "shortbread tin"],
        "Tortillas & Wraps": ["tortilla", "wrap", "flatbread", "naan"],
        "Breakfast Bakery": ["english muffin", "breakfast pastry", "coffee cake", "quiche"],
    },
    "Prepared Foods": {
        "Kimchi & Banchan": ["kimchi", "banchan"],
        "Deli Sides & Salads": ["side dish", "sidedish", "deli", "deli salad"],
        "Rice Meals & Kimbap": ["kimbap", "gimbap", "rice bowl", "fried rice", "sushi"],
        "Dumplings & Quick Meals": ["dumpling", "mandu", "tteokbokki", "katsu", "instant food", "ready meal"],
        "Soups & Stews": ["stew", "prepared soup", "ready soup"],
        "Prepared Proteins": ["prepared chicken", "prepared salmon", "prepared protein"],
        "Meal Kits": ["meal kit"],
    },
    "Frozen": {
        "Frozen Meals": ["frozen meal", "entree", "frozen entree"],
        "Frozen Breakfast": ["frozen waffle", "waffle", "frozen pancake", "breakfast burrito", "breakfast sandwich"],
        "Frozen Pizza": ["pizza", "flatbread pizza"],
        "Frozen Desserts": ["gelato", "sorbet", "frozen dessert", "ice pop", "popsicle"],
        "Frozen Produce": ["frozen fruit", "frozen vegetable"],
        "Ice Cream & Novelties": ["ice cream", "ice cream sandwich", "novelty"],
    },
    "Snacks": {
        "Candy & Gummies": ["candy", "gummy", "fruit snacks", "chews", "chocolate"],
        "Chips & Crackers": ["chips", "cracker", "pretzel", "popcorn", "crisps", "tortilla chips", "puffs", "rolled tortilla chips", "matzo", "matzo-style"],
        "Cookies & Sweet Snacks": ["cookies", "cookie", "bites", "wafel", "wafels", "wafer", "wafers", "stroopwafel", "biscuit", "biscuits"],
        "Bars": ["granola bar", "protein bar", "snack bar", "bar"],
        "Nuts & Trail Mix": ["nuts", "trail mix", "almonds", "cashews", "pistachio"],
        "Jerky & Savory Protein Snacks": ["jerky", "meat stick", "protein crisps"],
        "Popcorn & Puffs": ["popcorn", "puffs", "cheese puffs"],
    },
    "Pantry": {
        "Pasta, Rice & Grains": ["pasta", "rice", "grain", "quinoa", "couscous", "farro"],
        "Sauces & Marinades": ["sauce", "marinade", "pasta sauce", "alfredo", "bolognese", "pesto"],
        "Broth, Soup & Stock": ["broth", "soup", "stock"],
        "Dressings & Mayo": ["dressing", "mayo", "mayonnaise", "vinaigrette", "aioli"],
        "Dips & Spreads": ["hummus", "hommus", "dip", "dips", "guacamole", "queso"],
        "Nut Butters & Sweet Spreads": ["peanut butter", "almond butter", "cashew butter", "seed butter", "sunflower butter", "jam", "fruit spread", "preserves", "honey"],
        "Condiments": ["mustard", "ketchup", "hot sauce", "soy sauce", "condiment", "salsa"],
        "Oils & Vinegars": ["vinegar", "oil", "olive oil", "avocado oil"],
        "Baking Ingredients": ["flour", "baking soda", "baking powder", "cocoa powder", "vanilla extract"],
        "Spices & Seasonings": ["spice", "seasoning", "rub"],
        "Canned & Jarred Goods": ["jarred", "canned", "bruschetta", "canned tomato", "tomato paste", "paste tomato", "pie filling"],
        "Beans, Lentils & Legumes": ["beans", "lentils", "legumes", "chickpeas"],
        "International Staples": ["miso", "gochujang", "rice paper", "curry paste", "noodle"],
        "Breakfast Pantry": ["oatmeal", "cereal", "granola", "overnight oats", "oats"],
        "Baking Mixes & Meal Kits": ["mix", "baking mix", "meal kit"],
    },
    "Beverages": {
        "Coffee": ["coffee", "cold brew", "espresso"],
        "Tea": ["tea", "matcha", "chai"],
        "Sparkling Water & Seltzer": ["seltzer", "sparkling water", "sparkling beverage"],
        "Still Water & Hydration": ["water", "hydration", "coconut water", "alkaline water"],
        "Juice & Smoothies": ["juice", "smoothie", "smoothies"],
        "Energy & Sports Drinks": ["energy drink", "sports drink", "electrolyte drink", "hydration mix"],
        "Soda & Soft Drinks": ["soda", "cola", "soft drink", "ginger ale", "root beer"],
        "Functional Beverages": ["kombucha", "functional beverage", "mushroom coffee", "adaptogen drink", "sparkling adaptogen", "ashwagandha", "elixir"],
        "Creamers": ["creamer", "coffee creamer"],
        "Drink Mixes": ["drink mix", "powder drink", "mix packet"],
        "Mocktails & Zero Proof": ["zero proof", "non-alcoholic", "non alcoholic", "mocktail"],
    },
    "Alcohol": {
        "Beer": ["beer", "lager", "ipa", "stout", "pilsner", "ale"],
        "Wine": ["wine", "cabernet", "merlot", "chardonnay", "riesling", "pinot", "sauvignon", "prosecco", "rose", "rosé"],
        "Hard Seltzer": ["hard seltzer"],
        "Cider": ["cider"],
        "Spirits": ["vodka", "whiskey", "whisky", "tequila", "rum", "gin", "bourbon", "mezcal", "soju", "sake"],
        "Cocktails & Mixers": ["cocktail", "spritz", "spiked", "martini", "canned cocktail"],
        "Non-Alcoholic Beer & Wine": ["non-alcoholic beer", "non alcoholic beer", "non-alcoholic wine", "non alcoholic wine"],
    },
    "Supplements & Wellness": {
        "Vitamins & Minerals": ["vitamin", "magnesium", "omega", "multivitamin", "calcium", "zinc"],
        "Digestive & Probiotics": ["enzyme", "digestive", "probiotic", "gut health"],
        "Protein & Collagen": ["collagen", "protein powder", "peptides"],
        "Sleep & Stress": ["sleep support", "stress support", "calm", "relaxation"],
        "Beauty From Within": ["hair growth", "skin support", "beauty from within", "biotin"],
        "Electrolytes & Hydration": ["electrolyte", "hydration", "rehydration"],
        "Functional Mushrooms": ["functional mushroom", "lion's mane", "cordyceps", "reishi", "chaga", "mushroom powder"],
        "Herbal Remedies": ["turmeric", "elderberry", "herbal remedy", "tincture", "extract"],
    },
    "Household": {
        "Cleaning": ["cleaner", "disinfect", "sponge", "surface cleaner"],
        "Dish & Laundry": ["laundry", "detergent", "dish soap"],
        "Paper & Trash": ["paper towel", "toilet paper", "trash bag"],
    },
    "Beauty & Personal Care": {
        "Sun Care": ["sunscreen", "sun care", "spf"],
        "Skin Care": ["serum", "cleanser", "moisturizer", "skin care", "face lotion"],
        "Body Care": ["body wash", "body lotion", "for body", "face and body"],
        "Hair Care": ["shampoo", "conditioner", "detangler", "hair styling"],
        "Oral Care": ["toothpaste", "mouthwash", "oral care"],
        "Soap & Deodorant": ["hand soap", "soap bar", "deodorant"],
        "Baby Care": ["baby lotion", "baby wash", "baby care"],
    },
}
SUBCATEGORY_TO_CATEGORY = {
    subcategory: category
    for category, subcategories in SUBCATEGORY_PROFILES.items()
    for subcategory in subcategories.keys()
}

DIRECT_CATEGORY_HINTS = [
    {
        "category": "Alcohol",
        "include": [
            "hard seltzer", "beer", "wine", "cider", "vodka", "whiskey", "whisky",
            "tequila", "rum", "gin", "bourbon", "cabernet", "merlot", "chardonnay",
            "riesling", "pinot", "sauvignon", "prosecco", "rose", "rosé", "lager",
            "ipa", "stout", "pilsner", "ale", "spritz", "spiked", "cocktail",
            "canned cocktail", "mezcal", "sake", "soju",
        ],
        "exclude": ["wine vinegar", "mocktail", "zero proof", "non alcoholic", "non-alcoholic"],
    },
    {
        "category": "Supplements & Wellness",
        "include": [
            "multivitamin", "vitamin", "multimineral", "supplement", "enzyme",
            "probiotic", "collagen", "protein powder", "electrolyte", "hair growth",
            "sleep support", "stress support", "digestive support", "functional mushroom",
            "propolis", "throat spray", "echinacea", "goldenseal", "elderberry",
            "immune support", "throat health", "sambucus", "licorice",
        ],
        "exclude": ["protein bar", "snack bar", "energy drink"],
    },
    {
        "category": "Beauty & Personal Care",
        "include": [
            "sunscreen", "body lotion", "face lotion", "for face", "for body",
            "face and body", "skin care", "hair care", "oral care", "baby care",
            "conditioner", "detangler", "curl", "hair styling", "cleanser", "water spray",
        ],
        "exclude": ["dish soap", "laundry"],
    },
    {
        "category": "Snacks",
        "include": [
            "tortilla chips", "rolled tortilla chips", "potato chips", "corn chips",
            "pretzels", "popcorn", "crisps", "crackers", "chickpea puffs", "puffs",
            "jerky", "granola bar", "protein bar", "matzo", "matzo-style", "wafel",
            "wafels", "wafer", "wafers", "stroopwafel", "cookies", "cookie", "biscuit",
        ],
        "exclude": ["chocolate chips", "baking chips"],
    },
    {
        "category": "Pantry",
        "include": [
            "dipping sauce", "dip", "sauce dip", "queso", "pasta sauce",
            "bolognese sauce", "alfredo", "dressing", "marinade", "avocado oil",
            "olive oil", "mayo", "mayonnaise", "vinaigrette", "aioli", "overnight oats",
            "oats", "broth", "stock", "lentils", "meal kit", "tomato paste",
            "pesto",
            "paste tomato", "matzo ball", "pie filling",
        ],
        "exclude": ["chip dipper", "frozen meal"],
    },
    {
        "category": "Pantry",
        "include": ["macaroni and cheese", "mac and cheese", "mac cheese", "shells and cheddar"],
        "exclude": ["frozen", "pizza"],
    },
    {
        "category": "Prepared Foods",
        "include": [
            "kimchi", "kimbap", "gimbap", "banchan", "side dish", "sidedish",
            "dumpling", "mandu", "tteokbokki", "deli salad", "ready meal", "quiche",
        ],
        "exclude": ["dish soap", "laundry"],
    },
    {
        "category": "Frozen",
        "include": ["frozen meal", "frozen pizza", "ice cream", "gelato", "sorbet"],
        "exclude": [],
    },
    {
        "category": "Beverages",
        "include": [
            "coffee", "tea", "juice", "sparkling water", "seltzer", "water",
            "smoothie", "kombucha", "drink mix", "mocktail", "zero proof",
            "non alcoholic", "non-alcoholic", "sports drink", "energy drink",
            "sparkling adaptogen", "adaptogen drink", "fluid ounce bottle",
        ],
        "exclude": ["wine vinegar", "beer", "hard seltzer", "throat spray", "elderberry syrup"],
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
        if len(tokens) == 2:
            variants.add(" ".join(reversed(tokens)))
        last = tokens[-1]
        plural_variants = set()
        if last.endswith("y") and len(last) > 1:
            plural_variants.add(last[:-1] + "ies")
        elif last.endswith("o") and len(last) > 1:
            plural_variants.add(last + "es")
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


def parse_price_sort_value(text):
    if not text:
        return math.inf
    match = re.search(r"\$([0-9]+(?:\.[0-9]+)?)", str(text))
    if not match:
        return math.inf
    return float(match.group(1))


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


def normalize_retailer(value=None, url=None, sources=None):
    candidate = str(value or "").strip()
    if candidate:
        key = normalize_text_key(candidate)
        if key in {"whole foods", "whole foods market", "wfm"}:
            return WHOLE_FOODS_RETAILER
        if key == "target":
            return TARGET_RETAILER
        if key in {"h mart", "hmart"}:
            return HMART_RETAILER
        if key != "unknown":
            return candidate

    url_text = str(url or "").lower()
    source_text = normalize_text_key(" ".join(sources or []))
    if "wholefoodsmarket.com" in url_text or any(source in source_text for source in ["flyer", "all deals", "search deals"]):
        return WHOLE_FOODS_RETAILER
    if "target.com" in url_text or "target deals" in source_text:
        return TARGET_RETAILER
    if "hmart" in url_text or "h mart deals" in source_text:
        return HMART_RETAILER
    return WHOLE_FOODS_RETAILER


def normalize_source_label(source):
    label = str(source or "").strip()
    mapping = {
        "Flyer": "Flyer",
        "All Deals": "All Deals",
        "Search Deals": "Search",
        "Target Deals": "Target",
        "H Mart Deals": "H Mart",
    }
    return mapping.get(label, label)


def build_store_offer(product, store_id=None, source=None):
    retailer = normalize_retailer(product.get("retailer"), product.get("url"), product.get("sources") or ([source] if source else []))
    if retailer != WHOLE_FOODS_RETAILER:
        return None

    offer_store_id = str(store_id or (product.get("available_store_ids") or DEFAULT_STORE_IDS)[0])
    return {
        "store_id": offer_store_id,
        "current_price": product.get("current_price"),
        "basis_price": product.get("basis_price"),
        "prime_price": product.get("prime_price"),
        "discount": product.get("discount"),
        "discount_percent": product.get("discount_percent") or 0,
        "unit_price": product.get("unit_price"),
        "url": product.get("url"),
        "sources": list(product.get("sources") or ([source] if source else [])),
    }


def merge_store_offers(existing_offers, incoming_offer):
    offers_by_store = {
        str(offer.get("store_id")): dict(offer)
        for offer in (existing_offers or [])
        if offer and offer.get("store_id")
    }
    if incoming_offer and incoming_offer.get("store_id"):
        store_id = str(incoming_offer["store_id"])
        current = offers_by_store.get(store_id, {})
        merged = dict(current)
        for key, value in incoming_offer.items():
            if key == "sources":
                sources = []
                for source in (current.get("sources") or []) + (value or []):
                    if source and source not in sources:
                        sources.append(source)
                merged["sources"] = sources
            elif value not in (None, "", []):
                merged[key] = value
        offers_by_store[store_id] = merged
    return list(offers_by_store.values())


def apply_primary_store_offer(product):
    offers = product.get("store_offers") or []
    if not offers:
        return product
    selected = None
    for store_id in DEFAULT_STORE_IDS:
        selected = next((offer for offer in offers if str(offer.get("store_id")) == str(store_id)), None)
        if selected:
            break
    selected = selected or offers[0]
    for key in ["current_price", "basis_price", "prime_price", "discount", "discount_percent", "unit_price", "url"]:
        if selected.get(key) not in (None, "", []):
            product[key] = selected[key]
    return product


def derive_brand(name, explicit_brand=None):
    if explicit_brand:
        return clean_source_brand_display(explicit_brand)

    if not name:
        return None

    all_caps_prefix = extract_all_caps_brand_prefix(name)
    if all_caps_prefix:
        return clean_brand_display(all_caps_prefix)

    canonical = canonical_brand_for_alias(name)
    if canonical:
        return clean_brand_display(canonical)

    brand_candidate = extract_brand_candidate(name)
    if brand_candidate:
        return clean_brand_display(trim_brand_candidate(brand_candidate))

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
    "bone-in", "boneless", "spiral-cut", "spiral", "cut", "sunscreen", "lotion", "spray", "face", "body",
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
    "bone", "boneless", "bone-in", "spiral", "spiral-cut", "cut", "select", "sunscreen", "lotion", "spray", "sensitive", "face", "body",
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
    "MaryRuth's": ["maryruth's", "maryruths", "maryruth", "mary ruth's", "mary ruths"],
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
    "classic", "coffee", "cookies", "crackers", "deluxe", "dressing", "drink", "frozen", "garlic", "growth",
    "homegrown", "item", "items", "liquid", "lotion", "mac", "macaroni", "medium", "mineral", "mix", "multivitamin",
    "oatmilk", "organic", "pasta", "pepper", "pizza", "plant", "poppers", "powder", "pretzels", "protein",
    "queso", "real", "roasted", "sauce", "shells", "snack", "soda", "soup", "spray", "super", "supplement",
    "sunscreen", "tomato", "uncured", "vitamin", "with", "for", "face", "body", "sensitive", "kids", "baby",
    "boneless", "bone-in", "spiral", "spiral-cut", "select", "smoked", "sliced", "vegan", "gluten", "free",
    "fig", "dark", "chocolate", "oatmeal", "overnight", "cashew", "almond", "blueberry", "honey",
    "vinaigrette", "mayo", "mayonnaise", "aioli", "onion", "throat", "caramelized", "salad", "avocado",
    "dipping", "lime", "rosemary", "leave-in", "mask", "defining", "gel", "cleanser", "milk", "ranch",
    "caesar", "greek", "goddess", "creamy",
    "grain-free", "grain", "ultimate", "hydration", "strawberry", "lemonade", "sport", "citrus", "fruit",
    "electrolyte", "tabs", "tablets", "mix", "cookie", "cookies",
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


def looks_all_caps_brand_token(token):
    cleaned = token.strip(" ,.:;()[]{}-")
    if not cleaned:
        return False
    if cleaned.lower() in BRAND_CONNECTORS:
        return True
    letters = re.sub(r"[^A-Za-z]", "", cleaned)
    if not letters:
        return cleaned.isupper()
    return letters.upper() == letters


def extract_all_caps_brand_prefix(name):
    if not name:
        return None

    tokens = re.split(r"\s+", name.strip())
    brand_tokens = []
    for token in tokens[:6]:
        cleaned = token.strip(" ,.:;()[]{}")
        if not cleaned:
            continue
        if looks_all_caps_brand_token(cleaned):
            brand_tokens.append(cleaned)
            continue
        break

    if not brand_tokens:
        return None

    candidate = trim_brand_candidate(" ".join(brand_tokens))
    if not candidate or candidate_is_generic_brand(candidate):
        return None
    return candidate


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
    if not candidate:
        return None

    remainder = strip_brand_from_name(name.strip(), candidate)
    normalized_remainder = normalize_text_key(remainder)
    if not remainder or normalized_remainder == normalize_text_key(name):
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?\s*(?:oz|fl oz|lb|lbs|count|ct|pack|ea)\b.*", normalized_remainder):
        return None
    if len(normalized_remainder.split()) <= 1 and len(normalized_remainder) <= 12:
        shrinking_tokens = candidate.split()
        while len(shrinking_tokens) > 1:
            shrinking_tokens.pop()
            smaller_candidate = trim_brand_candidate(" ".join(shrinking_tokens))
            smaller_remainder = strip_brand_from_name(name.strip(), smaller_candidate)
            smaller_remainder_key = normalize_text_key(smaller_remainder)
            if len(smaller_remainder_key.split()) >= 2 and smaller_remainder_key != normalize_text_key(name):
                candidate = smaller_candidate
                normalized_remainder = smaller_remainder_key
                break

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


def clean_brand_display(brand, canonicalize=True):
    if not brand:
        return brand

    brand = re.sub(r"\s+", " ", brand).strip(" ,")
    canonical = canonical_brand_for_alias(brand) if canonicalize else None
    if canonical:
        brand = canonical

    tokens = re.split(r"(\s+)", brand)
    formatted = []
    seen_word = False
    for token in tokens:
        if not token or token.isspace():
            formatted.append(token)
            continue

        match = re.match(r"^([^A-Za-z0-9]*)([A-Za-z0-9’'&+/-]+)([^A-Za-z0-9]*)$", token)
        if not match:
            formatted.append(token)
            continue

        prefix, core, suffix = match.groups()
        if core.isupper():
            letters = re.sub(r"[^A-Z]", "", core)
            if any(ch.isdigit() for ch in core) or "&" in core or 1 < len(letters) <= 3:
                formatted_core = core
            else:
                formatted_core = title_case_token(core.lower(), is_first=not seen_word)
        elif core.islower():
            formatted_core = title_case_token(core, is_first=not seen_word)
        elif any(ch.isupper() for ch in core[1:]) and any(ch.islower() for ch in core):
            formatted_core = core[0].upper() + core[1:]
        else:
            formatted_core = title_case_token(core, is_first=not seen_word)
        formatted.append(f"{prefix}{formatted_core}{suffix}")
        seen_word = True

    return "".join(formatted).strip()


def clean_source_brand_display(brand):
    if not brand:
        return None

    cleaned_brand = re.sub(r"\s+", " ", str(brand)).strip(" ,")
    cleaned_brand = re.sub(r"^[®™©\s]+", "", cleaned_brand).strip(" ,")
    normalized_brand = normalize_text_key(cleaned_brand)
    if not normalized_brand:
        return None
    if "suppliers may vary" in normalized_brand:
        return None
    if candidate_is_generic_brand(cleaned_brand):
        return None
    return clean_brand_display(cleaned_brand, canonicalize=False)


def extend_incomplete_brand_with_name(brand, source_name):
    if not brand or not source_name:
        return brand

    brand_tokens = brand.split()
    if not brand_tokens:
        return brand
    if normalize_text_key(brand_tokens[-1]) not in BRAND_CONNECTORS:
        return brand

    remainder = strip_brand_from_name(source_name, brand)
    if not remainder or remainder == source_name:
        return brand

    first_token = remainder.split()[0].strip(" ,.:;()[]{}")
    first_key = normalize_text_key(first_token)
    if not first_key or any(ch.isdigit() for ch in first_token):
        return brand
    last_key = normalize_text_key(brand_tokens[-1])
    if last_key in {"co", "company", "food", "foods"} and (
        first_key in BRAND_DESCRIPTOR_STARTERS or first_key in GENERIC_BRAND_WORDS
    ):
        return brand

    extended = trim_brand_candidate(f"{brand} {first_token}")
    if not extended or candidate_is_generic_brand(extended):
        return brand
    return clean_brand_display(extended)


def brand_variants(brand):
    if not brand:
        return []

    variants = {brand.strip()}
    canonical = canonical_brand_for_alias(brand) or brand
    variants.add(canonical)

    for alias in BRAND_FAMILY_ALIASES.get(canonical, []):
        variants.add(alias)

    return [variant for variant in sorted(variants, key=len, reverse=True) if variant]


def strip_brand_from_name(name, brand):
    if not name or not brand:
        return name

    remainder = name
    stripped_any = False

    def starts_with_brand_variant(text, variant):
        if not text.lower().startswith(variant.lower()):
            return False
        if len(text) == len(variant):
            return True
        next_character = text[len(variant)]
        return not next_character.isalnum()

    while True:
        matched_variant = None
        for variant in brand_variants(brand):
            if starts_with_brand_variant(remainder, variant):
                matched_variant = variant
                break
        if not matched_variant:
            break

        stripped_any = True
        remainder = remainder[len(matched_variant):].lstrip(" ,:-–—")
        connector_pattern = r"^(?:" + "|".join(re.escape(word) for word in sorted(BRAND_CONNECTORS, key=len, reverse=True)) + r")\b[\s,:-]*"
        while True:
            updated = re.sub(connector_pattern, "", remainder, flags=re.IGNORECASE)
            if updated == remainder:
                break
            remainder = updated.lstrip(" ,:-–—")
            if not remainder:
                return name

    if not stripped_any:
        return name

    normalized_remainder = normalize_text_key(remainder)
    if normalized_remainder in GENERIC_REMAINDER_NAMES:
        return name

    if re.fullmatch(r"\d+(?:\.\d+)?\s*(?:oz|fl oz|lb|lbs|count|ct|pack)\b.*", normalized_remainder):
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

    if brand and normalize_text_key(cleaned) == normalize_text_key(clean_brand_display(brand)):
        return smart_title_case(name)

    return smart_title_case(cleaned)


def normalize_brands_across_products(products):
    family_map = {}
    source_brand_roots = sorted(
        {
            clean_source_brand_display(product.get("source_brand") or product.get("brand"))
            for product in products
            if product.get("brand_source") == "source" and (product.get("source_brand") or product.get("brand"))
        },
        key=lambda brand: (len(normalize_text_key(brand).split()), len(normalize_text_key(brand))),
    )
    unique_brands = sorted(
        {
            product.get("brand")
            for product in products
            if product.get("brand") and product.get("brand_source") != "source"
        },
        key=lambda brand: (len(normalize_text_key(brand).split()), len(normalize_text_key(brand))),
    )

    for brand in unique_brands:
        brand_key = normalize_text_key(brand)
        for source_brand in source_brand_roots:
            source_key = normalize_text_key(source_brand)
            if not source_key or brand_key == source_key:
                continue
            if not brand_key.startswith(source_key + " "):
                continue

            remainder = brand_key[len(source_key):].strip()
            remainder_first = remainder.split()[0] if remainder else ""
            if remainder_first in BRAND_DESCRIPTOR_STARTERS or remainder_first in GENERIC_BRAND_WORDS:
                family_map[brand] = source_brand
                break

        if brand in family_map:
            continue

        for candidate in unique_brands:
            if brand == candidate:
                continue
            candidate_key = normalize_text_key(candidate)
            if not candidate_key.startswith(brand_key + " "):
                continue

            remainder = candidate_key[len(brand_key):].strip()
            remainder_first = remainder.split()[0] if remainder else ""
            if remainder_first in BRAND_DESCRIPTOR_STARTERS or remainder_first in GENERIC_BRAND_WORDS:
                family_map[candidate] = brand

    derived_brand_products = [product for product in products if product.get("brand_source") != "source"]
    ai_family_map, _ = build_brand_family_map(
        derived_brand_products,
        alias_map=BRAND_FAMILY_ALIASES,
        connectors=BRAND_CONNECTORS,
        generic_words=GENERIC_BRAND_WORDS,
        descriptor_starters=BRAND_DESCRIPTOR_STARTERS,
    ) if derived_brand_products else ({}, None)
    family_map.update(ai_family_map)

    for product in products:
        if product.get("brand_source") == "source":
            source_brand = clean_source_brand_display(product.get("source_brand") or product.get("brand"))
            product["brand"] = source_brand
            source_name = product.get("raw_name") or product.get("name")
            if source_name:
                product["name"] = clean_display_name(source_name, product["brand"])
            product["tags"] = derive_tags(
                name=product.get("name"),
                brand=product.get("brand"),
                category=product.get("category"),
                sources=product.get("sources"),
                source_count=product.get("source_count", 0),
                prime_price=product.get("prime_price"),
            )
            continue

        original_brand = trim_brand_candidate(product.get("brand")) if product.get("brand") else None
        canonical_brand = family_map.get(original_brand, original_brand)
        brand_was_canonicalized = canonical_brand != original_brand
        if canonical_brand and candidate_is_generic_brand(canonical_brand):
            canonical_brand = None

        source_name = product.get("raw_name") or product.get("name")
        if canonical_brand and source_name and not brand_was_canonicalized:
            canonical_brand = extend_incomplete_brand_with_name(canonical_brand, source_name)

        product["brand"] = clean_brand_display(canonical_brand) if canonical_brand else None
        if source_name:
            product["name"] = clean_display_name(source_name, product["brand"])
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
    haystack = normalize_text_key(" ".join(filter(None, [name, variation, url])))
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
    scored_subcategories = score_subcategories(category, haystack)
    return scored_subcategories[0][0] if scored_subcategories and scored_subcategories[0][1] > 0 else None


def score_subcategories(category, haystack):
    subcategories = SUBCATEGORY_PROFILES.get(category, {})
    scores = []

    for subcategory, phrases in subcategories.items():
        score = 0
        for phrase in phrases:
            if text_contains_phrase(haystack, phrase):
                score += 4 if " " in normalize_text_key(phrase) else 2
        if category == "Produce":
            if any(text_contains_phrase(haystack, phrase) for phrase in ["fluid ounce", "bottle", "can", "drink", "juice", "seltzer", "water"]):
                score -= 6
        elif category == "Bakery":
            if any(text_contains_phrase(haystack, phrase) for phrase in ["cookie", "cookies", "cracker", "crackers", "wafel", "wafels", "wafer", "snack"]):
                if subcategory in {"Bread & Bagels", "Pastries & Desserts", "Breakfast Bakery"}:
                    score -= 4
        scores.append((subcategory, score))

    scores.sort(key=lambda item: (-item[1], item[0]))
    return scores


def score_all_categories(haystack):
    scored = []
    for category, profile in CATEGORY_PROFILES.items():
        score, reasons = score_category_profile(haystack, profile)
        scored.append(
            {
                "category": category,
                "score": score,
                "reasons": reasons,
            }
        )
    scored.sort(key=lambda item: (-item["score"], item["category"]))
    return scored


def derive_category_candidates(name, brand=None, variation=None, url=None, preferred_category=None):
    haystack = build_classification_haystack(name=name, brand=brand, variation=variation, url=url)
    if not haystack:
        return [preferred_category] if preferred_category else ["Pantry"]

    hinted_categories = []
    for hint in DIRECT_CATEGORY_HINTS:
        if any(text_contains_phrase(haystack, phrase) for phrase in hint["include"]) and not any(
            text_contains_phrase(haystack, phrase) for phrase in hint.get("exclude", [])
        ):
            hinted_categories.append(hint["category"])

    scored_categories = score_all_categories(haystack)
    top_score = scored_categories[0]["score"] if scored_categories else 0

    candidates = []
    if preferred_category:
        candidates.append(preferred_category)
    candidates.extend(hinted_categories)

    if top_score > 0:
        for item in scored_categories:
            if item["score"] <= 0:
                continue
            if item["score"] >= max(2, top_score - 4):
                candidates.append(item["category"])

    if not candidates:
        candidates.append(preferred_category or "Pantry")

    ordered_candidates = []
    for category in candidates:
        if category and category not in ordered_candidates:
            ordered_candidates.append(category)

    return ordered_candidates[:3]


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

    scored_categories = score_all_categories(haystack)
    best_category = scored_categories[0]["category"] if scored_categories else "Pantry"
    best_score = scored_categories[0]["score"] if scored_categories else -999
    second_best_score = scored_categories[1]["score"] if len(scored_categories) > 1 else -999
    best_reasons = scored_categories[0]["reasons"] if scored_categories else []

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
    source_brand=None,
    brand_source=None,
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
    if brand_source == "source":
        normalized_source_brand = clean_source_brand_display(source_brand or brand)
        normalized_brand = normalized_source_brand
        normalized_brand_source = "source" if normalized_source_brand else None
    elif brand_source == "derived":
        normalized_source_brand = None
        normalized_brand = derive_brand(source_name)
        normalized_brand_source = "derived" if normalized_brand else None
    else:
        normalized_source_brand = clean_source_brand_display(brand)
        if normalized_source_brand:
            normalized_brand = normalized_source_brand
            normalized_brand_source = "source"
        else:
            normalized_brand = derive_brand(source_name)
            normalized_brand_source = "derived" if normalized_brand else None
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
        "source_brand": normalized_source_brand,
        "brand_source": normalized_brand_source,
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
        "store_offers": [],
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
                extra_fields={
                    "retailer": p.get("retailer") or WHOLE_FOODS_RETAILER,
                    "available_store_ids": list(p.get("available_store_ids") or []),
                    "store_offers": list(p.get("store_offers") or []),
                    "source_store_id": p.get("source_store_id"),
                    "source_store_name": p.get("source_store_name"),
                },
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
        source_categories = []
        brand = p.get("brand")
        if normalize_text_key(brand) == "fresh produce":
            source_categories.append("Fresh Produce")
            brand = None
        product = standardize_product_record(
            asin=p.get("asin"),
            name=p.get("name"),
            raw_name=p.get("raw_name"),
            brand=brand,
            variation=p.get("variation"),
            image=p.get("image"),
            url=p.get("url"),
            unit_price=p.get("unit_price"),
            current_price=p.get("current_price"),
            regular_price=p.get("basis_price"),
            prime_price=p.get("prime_price"),
            discount_text=p.get("discount"),
            emoji=p.get("emoji"),
            classification_context=source_categories,
            extra_fields={
                "retailer": p.get("retailer") or "Whole Foods",
                "source_categories": source_categories,
                "available_store_ids": list(p.get("available_store_ids") or []),
                "store_offers": list(p.get("store_offers") or []),
                "source_store_id": p.get("source_store_id"),
                "source_store_name": p.get("source_store_name"),
            },
        )
        if source_categories:
            product["brand"] = None
            product["source_brand"] = None
            product["brand_source"] = None
        products.append(product)

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
        extra_fields = {
            "rank": p.get("rank"),
            "sale_price": p.get("sale_price"),
        }
        for key in [
            "retailer",
            "source_categories",
            "available_store_ids",
            "store_offers",
            "source_store_id",
            "source_store_name",
            "flyer_rank",
            "flyer_promotion_id",
            "flyer_promotion_grouping",
            "flyer_promotion_name",
            "flyer_detail_count",
            "flyer_source",
        ]:
            if p.get(key) is not None:
                extra_fields[key] = p.get(key)

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
                classification_context=p.get("source_categories"),
                extra_fields=extra_fields,
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
        merged["retailer"] = normalize_retailer(merged.get("retailer"), merged.get("url"), merged.get("sources"))
        merged["sources"] = list(incoming.get("sources", []))
        merged["source_labels"] = [normalize_source_label(source) for source in merged["sources"]]
        merged["source_count"] = len(merged["sources"])
        merged["store_offers"] = []
        incoming_offers = list(incoming.get("store_offers") or [])
        fallback_offer = build_store_offer(merged, source=(merged["sources"][0] if merged["sources"] else None))
        if fallback_offer and not incoming_offers:
            incoming_offers = [fallback_offer]
        for offer in incoming_offers:
            merged["store_offers"] = merge_store_offers(merged["store_offers"], offer)
        apply_primary_store_offer(merged)
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
        if key in {"sources", "source_labels", "tags", "available_store_ids", "store_offers"}:
            continue
        if value in (None, "", []):
            continue
        if merged.get(key) in (None, "", []):
            merged[key] = value

    incoming_has_source_brand = incoming.get("brand") and incoming.get("brand_source") == "source"
    existing_has_source_brand = merged.get("brand") and merged.get("brand_source") == "source"
    incoming_brand_key = normalize_text_key(incoming.get("source_brand") or incoming.get("brand"))
    existing_brand_key = normalize_text_key(merged.get("source_brand") or merged.get("brand"))
    incoming_source_brand_is_better = (
        incoming_has_source_brand
        and existing_has_source_brand
        and incoming_brand_key.startswith(existing_brand_key + " ")
    )
    if incoming_has_source_brand and (not existing_has_source_brand or incoming_source_brand_is_better):
        merged["brand"] = incoming.get("brand")
        merged["source_brand"] = incoming.get("source_brand") or incoming.get("brand")
        merged["brand_source"] = "source"
        source_name = merged.get("raw_name") or incoming.get("raw_name") or incoming.get("name")
        if source_name:
            merged["name"] = clean_display_name(source_name, merged["brand"])

    incoming_source_categories = incoming.get("source_categories") or []
    if any(normalize_text_key(category) == "fresh produce" for category in incoming_source_categories):
        merged["source_categories"] = sorted(set((merged.get("source_categories") or []) + incoming_source_categories))
        if merged.get("brand_source") != "source":
            merged["brand"] = None
            merged["source_brand"] = None
            merged["brand_source"] = None

    merged_sources = []
    for source in existing.get("sources", []) + incoming.get("sources", []):
        if source not in merged_sources:
            merged_sources.append(source)

    merged["sources"] = merged_sources
    merged["source_labels"] = [normalize_source_label(source) for source in merged_sources]
    merged["source_count"] = len(merged_sources)
    merged_store_ids = []
    for store_id in existing.get("available_store_ids", []) + incoming.get("available_store_ids", []):
        if store_id not in merged_store_ids:
            merged_store_ids.append(store_id)
    merged["available_store_ids"] = merged_store_ids or list(DEFAULT_STORE_IDS)
    merged["retailer"] = normalize_retailer(merged.get("retailer"), merged.get("url"), merged_sources)
    merged["store_offers"] = list(existing.get("store_offers") or [])
    for offer in incoming.get("store_offers") or [build_store_offer(incoming, source=(incoming.get("sources") or [None])[0])]:
        merged["store_offers"] = merge_store_offers(merged.get("store_offers"), offer)
    apply_primary_store_offer(merged)
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


def api_url(path):
    if PUBLIC_API_BASE_URL:
        return f"{PUBLIC_API_BASE_URL}{path}"
    return path


def subcategory_signature(product):
    retailer = normalize_text_key(product.get("retailer") or "Whole Foods")
    brand = normalize_text_key(product.get("brand"))
    subcategory = normalize_text_key(product.get("subcategory") or product.get("category") or "Pantry")
    if brand:
        return f"subcategory:{retailer}:{brand}:{subcategory}"
    return f"subcategory:{retailer}:{subcategory}"


def brand_signature(product):
    retailer = normalize_text_key(product.get("retailer") or "Whole Foods")
    brand = normalize_text_key(product.get("brand"))
    if brand:
        return f"brand:{retailer}:{brand}"
    return f"name:{retailer}:{normalize_text_key(product.get('raw_name') or product.get('name'))}"


def normalize_review_key(value):
    return " ".join(str(value or "").lower().split())


def gold_label_key(label):
    for field in ("asin", "url", "raw_name", "name"):
        value = label.get(field)
        if value:
            return f"{field}:{normalize_review_key(value)}"
    return None


def default_gold_labels_payload():
    return {
        "instructions": (
            "Manual taxonomy labels. These override exact products and are weighted into "
            "the local sklearn taxonomy classifier during refresh."
        ),
        "label_count": 0,
        "labels": [],
    }


def load_gold_labels_payload():
    try:
        with open(TAXONOMY_GOLD_LABELS_FILE, "r", encoding="utf-8") as gold_file:
            payload = json.load(gold_file)
    except FileNotFoundError:
        return default_gold_labels_payload()
    except json.JSONDecodeError:
        return default_gold_labels_payload()

    if not isinstance(payload, dict):
        return default_gold_labels_payload()
    labels = payload.get("labels")
    if not isinstance(labels, list):
        payload["labels"] = []
    payload.setdefault("instructions", default_gold_labels_payload()["instructions"])
    payload["label_count"] = len(payload.get("labels") or [])
    return payload


def save_gold_labels_payload(payload):
    normalized = default_gold_labels_payload()
    labels = payload.get("labels") if isinstance(payload, dict) else []
    if isinstance(labels, list):
        normalized["labels"] = labels
    normalized["label_count"] = len(normalized["labels"])
    instructions = (payload or {}).get("instructions") if isinstance(payload, dict) else ""
    if instructions:
        normalized["instructions"] = instructions
    with open(TAXONOMY_GOLD_LABELS_FILE, "w", encoding="utf-8") as gold_file:
        json.dump(normalized, gold_file, indent=2, ensure_ascii=False)
        gold_file.write("\n")


def default_fixes_to_deploy():
    return {
        "subcategory_overrides_by_key": {},
        "subcategory_overrides_by_signature": {},
        "brand_overrides_by_key": {},
        "brand_overrides_by_signature": {},
    }


def load_fixes_to_deploy():
    remote_fixes = load_fixes_from_supabase()
    if remote_fixes:
        return remote_fixes

    try:
        with open(FIXES_TO_DEPLOY_FILE, "r", encoding="utf-8") as fixes_file:
            data = json.load(fixes_file)
    except FileNotFoundError:
        return default_fixes_to_deploy()
    except json.JSONDecodeError:
        return default_fixes_to_deploy()

    fixes = default_fixes_to_deploy()
    for key, default_value in fixes.items():
        value = data.get(key)
        if isinstance(default_value, dict) and isinstance(value, dict):
            fixes[key] = value
    return fixes


def save_fixes_to_deploy(fixes):
    with open(FIXES_TO_DEPLOY_FILE, "w", encoding="utf-8") as fixes_file:
        json.dump(fixes, fixes_file, indent=2, ensure_ascii=False)


def default_device_profiles():
    return {}


def load_device_profiles_local():
    try:
        with open(DEVICE_PROFILES_FILE, "r", encoding="utf-8") as profiles_file:
            data = json.load(profiles_file)
    except FileNotFoundError:
        return default_device_profiles()
    except json.JSONDecodeError:
        return default_device_profiles()

    if not isinstance(data, dict):
        return default_device_profiles()
    return data


def save_device_profiles_local(profiles):
    with open(DEVICE_PROFILES_FILE, "w", encoding="utf-8") as profiles_file:
        json.dump(profiles, profiles_file, indent=2, ensure_ascii=False)


def load_device_profile(device_id):
    remote_profile = load_device_profile_from_supabase(device_id)
    if remote_profile:
        return remote_profile

    profiles = load_device_profiles_local()
    profile = profiles.get(device_id)
    if not isinstance(profile, dict):
        return None
    return {
        "selectedStoreIds": profile.get("selectedStoreIds") or [],
        "likedKeys": profile.get("likedKeys") or [],
        "dislikedKeys": profile.get("dislikedKeys") or [],
        "savedKeys": profile.get("savedKeys") or [],
        "categoryOrderByRetailer": profile.get("categoryOrderByRetailer") or {},
    }


def save_device_profile(device_id, profile):
    normalized = {
        "selectedStoreIds": profile.get("selectedStoreIds") or [],
        "likedKeys": profile.get("likedKeys") or [],
        "dislikedKeys": profile.get("dislikedKeys") or [],
        "savedKeys": profile.get("savedKeys") or [],
        "categoryOrderByRetailer": profile.get("categoryOrderByRetailer") or {},
    }

    if save_device_profile_to_supabase(device_id, normalized):
        return normalized

    profiles = load_device_profiles_local()
    profiles[device_id] = normalized
    save_device_profiles_local(profiles)
    return normalized


def sorted_count_map(values):
    counter = Counter(value for value in values if value)
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def apply_subcategory_ai(products):
    if not products:
        return products

    valid_subcategories = set(SUBCATEGORY_TO_CATEGORY.keys())
    training_products = [
        product for product in products
        if (
            product.get("subcategory") in valid_subcategories
            and (
                "queued fix" in (product.get("category_signals") or [])
                or float(product.get("category_confidence") or 0) >= 0.85
            )
        )
    ]

    model, metadata = train_subcategory_model(training_products, valid_subcategories)
    metadata = dict(metadata or {})
    metadata["model_version"] = SUBCATEGORY_AI_MODEL_VERSION
    metadata["valid_subcategory_count"] = len(valid_subcategories)
    metadata["minimum_confidence_for_override"] = SUBCATEGORY_AI_MIN_CONFIDENCE
    metadata["training_retailer_counts"] = sorted_count_map(
        product.get("retailer") for product in training_products
    )
    metadata["training_category_counts"] = sorted_count_map(
        product.get("category") for product in training_products
    )

    if model is not None:
        save_subcategory_ai_artifacts(
            model,
            metadata,
            SUBCATEGORY_AI_MODEL_FILE,
            SUBCATEGORY_AI_METADATA_FILE,
        )
    else:
        cached_model, cached_metadata = load_subcategory_ai_artifacts(
            SUBCATEGORY_AI_MODEL_FILE,
            SUBCATEGORY_AI_METADATA_FILE,
        )
        if cached_model is not None:
            model = cached_model
            metadata = dict(cached_metadata or {})
            metadata["loaded_from_disk"] = True
        else:
            save_subcategory_ai_artifacts(
                None,
                metadata,
                SUBCATEGORY_AI_MODEL_FILE,
                SUBCATEGORY_AI_METADATA_FILE,
            )

    allowed_subcategories = []
    subcategory_priors = []
    for product in products:
        haystack = build_classification_haystack(
            name=product.get("raw_name") or product.get("name"),
            brand=product.get("brand"),
            variation=" ".join(
                part for part in [
                    product.get("variation"),
                    " ".join(product.get("source_categories") or []),
                ]
                if part
            ),
            url=product.get("url"),
        )
        candidate_categories = derive_category_candidates(
            name=product.get("raw_name") or product.get("name"),
            brand=product.get("brand"),
            variation=" ".join(
                part for part in [
                    product.get("variation"),
                    " ".join(product.get("source_categories") or []),
                ]
                if part
            ),
            url=product.get("url"),
        )
        allowed = []
        prior_scores = {}
        for category in candidate_categories:
            allowed.extend(SUBCATEGORY_PROFILES.get(category, {}).keys())
            for subcategory, score in score_subcategories(category, haystack):
                if score > 0:
                    prior_scores[subcategory] = max(prior_scores.get(subcategory, 0), score)
        allowed_subcategories.append(sorted(dict.fromkeys(allowed)))
        subcategory_priors.append(prior_scores)

    predictions = predict_subcategories(
        model,
        products,
        allowed_subcategories=allowed_subcategories,
        subcategory_priors=subcategory_priors,
    ) if model is not None else []

    for index, product in enumerate(products):
        product["previous_category"] = product.get("category")
        product["previous_subcategory"] = product.get("subcategory")

        prediction = predictions[index] if index < len(predictions) else None

        final_subcategory = None
        final_confidence = 0.0
        label_source = "model"

        if (
            prediction
            and prediction.get("subcategory") in valid_subcategories
            and (prediction.get("confidence") or 0) >= SUBCATEGORY_AI_MIN_CONFIDENCE
        ):
            final_subcategory = prediction["subcategory"]
            final_confidence = prediction.get("confidence") or 0
        else:
            existing_subcategory = product.get("subcategory")
            if existing_subcategory in valid_subcategories:
                final_subcategory = existing_subcategory
                final_confidence = float(product.get("category_confidence") or 0)

        if final_subcategory in valid_subcategories:
            final_category = SUBCATEGORY_TO_CATEGORY[final_subcategory]
            product["category"] = final_category
            product["subcategory"] = final_subcategory

        signals = list(product.get("category_signals") or [])
        if "subcategory ai" not in signals:
            signals.insert(0, "subcategory ai")
        product["category_signals"] = signals[:6]

        product["category_confidence"] = round(float(final_confidence or 0), 4)
        product["ai_subcategory"] = product.get("subcategory")
        product["ai_category"] = product.get("category")
        product["ai_confidence"] = round(float(final_confidence or 0), 4)
        product["ai_label_source"] = label_source
        product["ai_model_version"] = metadata.get("model_version") or SUBCATEGORY_AI_MODEL_VERSION
        product["tags"] = derive_tags(
            name=product.get("name"),
            brand=product.get("brand"),
            category=product.get("category"),
            sources=product.get("sources"),
            source_count=product.get("source_count", 0),
            prime_price=product.get("prime_price"),
        )

    report = build_subcategory_ai_change_report(products)
    report["training"] = metadata
    report["final_category_counts"] = sorted_count_map(
        product.get("category") for product in products
    )
    report["final_subcategory_counts"] = sorted_count_map(
        product.get("subcategory") for product in products
    )
    with open(SUBCATEGORY_AI_REPORT_FILE, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, ensure_ascii=False)

    for product in products:
        product.pop("previous_category", None)
        product.pop("previous_subcategory", None)

    return products


def apply_fixes_to_products(products):
    fixes = load_fixes_to_deploy()
    subcategory_by_key = fixes.get("subcategory_overrides_by_key", {})
    subcategory_by_signature = fixes.get("subcategory_overrides_by_signature", {})
    brand_by_key = fixes.get("brand_overrides_by_key", {})
    brand_by_signature = fixes.get("brand_overrides_by_signature", {})

    for product in products:
        item_key = combined_key_for_product(product)

        brand_override = brand_by_key.get(item_key) or brand_by_signature.get(brand_signature(product))
        if brand_override:
            normalized_brand = clean_brand_display(trim_brand_candidate(brand_override) or brand_override)
            product["brand"] = normalized_brand
            product["name"] = clean_display_name(product.get("raw_name") or product.get("name"), normalized_brand)

        subcategory_override = (
            subcategory_by_key.get(item_key)
            or subcategory_by_signature.get(subcategory_signature(product))
        )
        if subcategory_override and subcategory_override in SUBCATEGORY_TO_CATEGORY:
            category = SUBCATEGORY_TO_CATEGORY[subcategory_override]
            product["category"] = category
            product["subcategory"] = subcategory_override
            product["category_confidence"] = 1.0
            signals = list(product.get("category_signals") or [])
            signals.insert(0, "queued fix")
            product["category_signals"] = signals[:6]
            product["ai_subcategory"] = subcategory_override
            product["ai_category"] = category
            product["ai_confidence"] = 1.0
            product["ai_label_source"] = "model"
            product["ai_model_version"] = SUBCATEGORY_AI_MODEL_VERSION

        product["tags"] = derive_tags(
            name=product.get("name"),
            brand=product.get("brand"),
            category=product.get("category"),
            sources=product.get("sources"),
            source_count=product.get("source_count", 0),
            prime_price=product.get("prime_price"),
        )
        product["ai_subcategory"] = product.get("ai_subcategory") or product.get("subcategory")
        product["ai_category"] = product.get("ai_category") or product.get("category")
        product["ai_confidence"] = float(
            product.get("ai_confidence") or product.get("category_confidence") or 0
        )
        product["ai_label_source"] = "model"
        product["ai_model_version"] = product.get("ai_model_version") or SUBCATEGORY_AI_MODEL_VERSION

    return products


def normalized_product_for_source(product, source_name):
    retailer = normalize_retailer(product.get("retailer"), product.get("url"), [source_name])
    store_ids = []
    if retailer == WHOLE_FOODS_RETAILER:
        store_ids = list(product.get("available_store_ids") or [])
        if not store_ids and source_name == "All Deals":
            store_ids = list(ACTIVE_WHOLE_FOODS_STORE_IDS or DEFAULT_STORE_IDS)
        if not store_ids:
            store_ids = list(DEFAULT_STORE_IDS)
    normalized = standardize_product_record(
        asin=product.get("asin"),
        asins=product.get("asins"),
        name=product.get("name"),
        raw_name=product.get("raw_name"),
        brand=product.get("brand"),
        source_brand=product.get("source_brand"),
        brand_source=product.get("brand_source"),
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
            "retailer": retailer,
            "expires": product.get("expires"),
            "retail_source_url": product.get("retail_source_url"),
            "source_categories": product.get("source_categories") or [],
            "available_store_ids": store_ids,
        },
    )
    normalized["sources"] = [source_name]
    normalized["source_labels"] = [normalize_source_label(source_name)]
    if retailer == WHOLE_FOODS_RETAILER:
        normalized["store_offers"] = [
            offer for offer in (
                build_store_offer(normalized, store_id=store_id, source=source_name)
                for store_id in store_ids
            )
            if offer
        ]
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
    force_taxonomy_rediscovery=False,
    taxonomy_sample_size=None,
    classify=True,
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
    for product in ordered:
        product["retailer"] = normalize_retailer(product.get("retailer"), product.get("url"), product.get("sources"))
    unknown_retailers = [product for product in ordered if not product.get("retailer") or normalize_text_key(product.get("retailer")) == "unknown"]
    if unknown_retailers:
        examples = ", ".join((product.get("name") or "Unnamed product") for product in unknown_retailers[:5])
        raise RuntimeError(f"Unknown retailers remain after normalization ({len(unknown_retailers)}): {examples}")
    previous_brand_signature = None
    for _ in range(4):
        ordered = normalize_brands_across_products(ordered)
        brand_signature = tuple(sorted({product.get("brand") or "" for product in ordered}))
        if brand_signature == previous_brand_signature:
            break
        previous_brand_signature = brand_signature
    if not classify:
        ordered.sort(
            key=lambda product: (
                -product.get("source_count", 0),
                normalize_text_key(product.get("name")),
            )
        )
        return ordered
    if taxonomy_sample_size:
        ordered = sample_products_for_taxonomy_testing(ordered, taxonomy_sample_size)
    ordered, taxonomy = classify_products_with_taxonomy_ai(
        BASE_DIR,
        ordered,
        force_rediscover=force_taxonomy_rediscovery,
    )
    taxonomy_categories = [category.get("name") for category in taxonomy.get("categories") or []]
    for product in ordered:
        if product.get("category") not in taxonomy_categories and taxonomy_categories:
            product["category"] = product.get("ai_category") or product.get("category") or taxonomy_categories[0]
    ordered.sort(
        key=lambda product: (
            -product.get("source_count", 0),
            normalize_text_key(product.get("name")),
        )
    )
    return ordered


def sample_products_for_taxonomy_testing(products, sample_size):
    try:
        sample_size = int(sample_size)
    except (TypeError, ValueError):
        return products
    if sample_size <= 0 or len(products) <= sample_size:
        return products

    by_retailer = defaultdict(list)
    for product in products:
        retailer = product.get("retailer") or "Unknown"
        by_retailer[retailer].append(product)

    for retailer_products in by_retailer.values():
        retailer_products.sort(
            key=lambda product: (
                normalize_text_key(product.get("name")),
                normalize_text_key(product.get("brand")),
                normalize_text_key(product.get("url")),
            )
        )

    sampled = []
    retailer_names = sorted(by_retailer)
    while len(sampled) < sample_size and retailer_names:
        next_round = []
        for retailer in retailer_names:
            bucket = by_retailer[retailer]
            if bucket and len(sampled) < sample_size:
                sampled.append(bucket.pop(0))
            if bucket:
                next_round.append(retailer)
        retailer_names = next_round

    return sampled


def hydrate_combined_product_record(product, index=0):
    hydrated = dict(product)
    hydrated["asin"] = hydrated.get("asin")
    hydrated["asins"] = list(hydrated.get("asins") or ([hydrated["asin"]] if hydrated.get("asin") else []))
    hydrated["name"] = hydrated.get("name") or hydrated.get("raw_name") or f"Product {index + 1}"
    hydrated["raw_name"] = hydrated.get("raw_name") or hydrated["name"]
    hydrated["brand"] = hydrated.get("brand") or None
    hydrated["source_brand"] = hydrated.get("source_brand") or None
    hydrated["brand_source"] = hydrated.get("brand_source") or ("source" if hydrated.get("source_brand") else None)
    hydrated["variation"] = hydrated.get("variation") or None
    hydrated["image"] = hydrated.get("image")
    hydrated["url"] = hydrated.get("url")
    hydrated["unit_price"] = hydrated.get("unit_price")
    hydrated["current_price"] = hydrated.get("current_price") or hydrated.get("sale_price")
    hydrated["basis_price"] = hydrated.get("basis_price")
    hydrated["prime_price"] = hydrated.get("prime_price")
    hydrated["discount"] = hydrated.get("discount")
    hydrated["retailer"] = normalize_retailer(hydrated.get("retailer"), hydrated.get("url"), hydrated.get("sources"))
    hydrated["category"] = hydrated.get("category") or "Pantry"
    hydrated["subcategory"] = hydrated.get("subcategory")
    hydrated["category_confidence"] = float(hydrated.get("category_confidence") or 0)
    hydrated["category_signals"] = list(hydrated.get("category_signals") or [])
    hydrated["ai_subcategory"] = hydrated.get("ai_subcategory") or hydrated.get("subcategory")
    hydrated["ai_category"] = hydrated.get("ai_category") or hydrated.get("category")
    hydrated["ai_confidence"] = float(hydrated.get("ai_confidence") or hydrated.get("category_confidence") or 0)
    hydrated["ai_reasoning"] = hydrated.get("ai_reasoning") or ""
    hydrated["ai_model_name"] = hydrated.get("ai_model_name") or TAXONOMY_AI_MODEL_NAME
    hydrated["ai_taxonomy_version"] = hydrated.get("ai_taxonomy_version")
    hydrated["ai_fingerprint"] = hydrated.get("ai_fingerprint")
    hydrated["ai_label_source"] = hydrated.get("ai_label_source") or "model"
    hydrated["ai_model_version"] = hydrated.get("ai_model_version") or TAXONOMY_AI_MODEL_VERSION
    hydrated["sources"] = list(hydrated.get("sources") or [])
    hydrated["source_labels"] = list(hydrated.get("source_labels") or [normalize_source_label(source) for source in hydrated["sources"]])
    hydrated["source_count"] = int(hydrated.get("source_count") or len(hydrated["sources"]))
    hydrated["tags"] = list(hydrated.get("tags") or [])
    default_store_ids = DEFAULT_STORE_IDS if hydrated["retailer"] == WHOLE_FOODS_RETAILER else []
    hydrated["available_store_ids"] = list(hydrated.get("available_store_ids") or default_store_ids)
    hydrated["store_offers"] = list(hydrated.get("store_offers") or [])
    if hydrated["retailer"] == WHOLE_FOODS_RETAILER and not hydrated["store_offers"]:
        for store_id in hydrated["available_store_ids"] or DEFAULT_STORE_IDS:
            offer = build_store_offer(hydrated, store_id=store_id, source=(hydrated["sources"][0] if hydrated["sources"] else None))
            if offer:
                hydrated["store_offers"] = merge_store_offers(hydrated["store_offers"], offer)
    apply_primary_store_offer(hydrated)
    if hydrated.get("basis_price") and hydrated.get("prime_price"):
        hydrated["prime_price"] = harmonize_prime_suffix(hydrated.get("basis_price"), hydrated.get("prime_price"))
    if not hydrated["tags"]:
        hydrated["tags"] = derive_tags(
            name=hydrated.get("name"),
            brand=hydrated.get("brand"),
            category=hydrated.get("category"),
            sources=hydrated.get("sources"),
            source_count=hydrated.get("source_count", 0),
            prime_price=hydrated.get("prime_price"),
        )
    apply_failed_classification_bucket(hydrated)
    return hydrated


@lru_cache(maxsize=1)
def load_base_combined_products():
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
    return [hydrate_combined_product_record(product, index) for index, product in enumerate(products)]


def load_combined_products():
    return [dict(product) for product in load_base_combined_products()]


def load_active_taxonomy():
    products = load_combined_products()
    return ensure_taxonomy(BASE_DIR, products, force_rediscover=False)


def active_taxonomy_category_names():
    taxonomy = load_active_taxonomy()
    return [category.get("name") for category in taxonomy.get("categories") or []]


def active_taxonomy_subcategory_options():
    taxonomy = load_active_taxonomy()
    return taxonomy_to_options(taxonomy)


def build_flyer_display_product(p):
    store_id = str(p.get("store_id") or DEFAULT_STORE_IDS[0])
    store_name = p.get("store_name")
    return standardize_product_record(
        name=p.get("productName", "Unknown Product"),
        brand=p.get("brandName") or p.get("originBrandName"),
        image=p.get("productImage"),
        regular_price=p.get("regularPrice"),
        current_price=p.get("salePrice"),
        prime_price=p.get("primePrice"),
        asins=p.get("asinsList", []),
        emoji=emoji_for_product(p.get("productName", "Unknown Product")),
        extra_fields={
            "rank": p.get("rank"),
            "sale_price": p.get("salePrice"),
            "available_store_ids": [store_id],
            "flyer_promotion_id": p.get("promotionId"),
            "flyer_promotion_grouping": p.get("promotionGrouping"),
            "flyer_source": "display-promotion",
            "source_store_id": store_id,
            "source_store_name": store_name,
            "retailer": "Whole Foods",
        },
    )


def build_flyer_promotion_url(p, store_id=None):
    promotion_id = p.get("promotionId")
    if not promotion_id:
        return None
    selected_store_id = str(store_id or p.get("store_id") or DEFAULT_STORE_IDS[0])
    return f"https://www.wholefoodsmarket.com/promotion/{promotion_id}?store-id={selected_store_id}"


def extract_flyer_asin_from_href(href):
    if not href:
        return None
    match = re.search(r"/grocery/product/([A-Z0-9]{10})", href, re.IGNORECASE)
    return match.group(1).upper() if match else None


def clean_flyer_tile_text(text):
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    cleaned = re.sub(r"\s+\*$", "", cleaned).strip()
    return cleaned or None


def parse_flyer_promotion_detail_products(html):
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return []

    soup = BeautifulSoup(html, "html.parser")
    products = []
    seen_asins = set()

    for link in soup.select('a[data-csa-c-type="productTile"][href*="/grocery/product/"]'):
        href = link.get("href") or ""
        asin = extract_flyer_asin_from_href(href)
        if not asin or asin in seen_asins:
            continue

        name = clean_flyer_tile_text(link.get("data-csa-c-content-id"))
        image = None
        image_el = link.find("img")
        if image_el:
            image = image_el.get("src")
            if not name:
                name = clean_flyer_tile_text(image_el.get("alt"))

        brand = None
        for span in link.find_all("span"):
            classes = " ".join(span.get("class") or [])
            text = clean_flyer_tile_text(span.get_text(" ", strip=True))
            if not text:
                continue
            if "bds--body-2" not in classes or "text-chia-seed" not in classes:
                continue
            if re.search(r"^(valid|exp\.?|sale bug)$", text, re.IGNORECASE):
                continue
            brand = text
            break

        if not name:
            continue

        products.append({
            "asin": asin,
            "asins": [asin],
            "name": name,
            "brand": brand,
            "image": image,
            "url": urljoin("https://www.wholefoodsmarket.com", href),
        })
        seen_asins.add(asin)

    return products


def expand_flyer_promotion_detail_page(page, max_clicks=20):
    product_tiles = page.locator('a[data-csa-c-type="productTile"][href*="/grocery/product/"]')
    load_more_clicks = 0
    stale_rounds = 0
    previous_count = product_tiles.count()

    while load_more_clicks < max_clicks:
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(700)
        except Exception:
            pass

        load_more = page.get_by_role("button", name=re.compile(r"load\s+more", re.IGNORECASE))
        if not load_more.count():
            load_more = page.locator("button", has_text=re.compile(r"load\s+more", re.IGNORECASE))

        if not load_more.count():
            break

        try:
            button = load_more.first
            if not button.is_enabled():
                break
            button.scroll_into_view_if_needed()
            button.click(timeout=10000)
            load_more_clicks += 1
            page.wait_for_timeout(1500)
        except Exception:
            break

        current_count = product_tiles.count()
        if current_count <= previous_count:
            stale_rounds += 1
        else:
            stale_rounds = 0
        previous_count = current_count

        if stale_rounds >= 2:
            break

    return load_more_clicks


def standardize_flyer_detail_product(p, detail_product, detail_index, detail_count):
    store_id = str(p.get("store_id") or DEFAULT_STORE_IDS[0])
    store_name = p.get("store_name")
    raw_brand = detail_product.get("brand") or p.get("brandName") or p.get("originBrandName")
    source_categories = []
    if normalize_text_key(raw_brand) == "fresh produce":
        source_categories.append("Fresh Produce")

    brand = None if source_categories else raw_brand
    rank = p.get("rank")
    detail_rank = (rank + (detail_index / 1000)) if isinstance(rank, int) else detail_index

    product = standardize_product_record(
        asin=detail_product.get("asin"),
        asins=detail_product.get("asins") or ([detail_product["asin"]] if detail_product.get("asin") else []),
        name=detail_product.get("name") or p.get("productName", "Unknown Product"),
        brand=brand,
        image=detail_product.get("image") or p.get("productImage"),
        url=detail_product.get("url") or build_flyer_promotion_url(p),
        regular_price=p.get("regularPrice"),
        current_price=p.get("salePrice"),
        prime_price=p.get("primePrice"),
        classification_context=source_categories,
        emoji=emoji_for_product(detail_product.get("name") or p.get("productName", "Unknown Product")),
        extra_fields={
            "rank": detail_rank,
            "flyer_rank": rank,
            "sale_price": p.get("salePrice"),
            "available_store_ids": [store_id],
            "flyer_promotion_id": p.get("promotionId"),
            "flyer_promotion_grouping": p.get("promotionGrouping"),
            "flyer_promotion_name": p.get("productName"),
            "flyer_detail_count": detail_count,
            "flyer_source": "promotion-detail",
            "source_store_id": store_id,
            "source_store_name": store_name,
            "retailer": "Whole Foods",
        },
    )

    if source_categories:
        product["source_categories"] = source_categories
        product["brand"] = None
        product["source_brand"] = None
        product["brand_source"] = None

    return product


def fetch_products(store=None):
    store_id = str((store or {}).get("id") or DEFAULT_STORE_IDS[0])
    store_name = (store or {}).get("name")
    sales_flyer_url = build_sales_flyer_url(store_id)

    r = requests.get(sales_flyer_url, timeout=20)
    r.raise_for_status()

    next_data = extract_next_data_from_html(r.text)

    promotions = (
        next_data.get("props", {}).get("pageProps", {}).get("promotions")
        or next_data.get("pageProps", {}).get("promotions")
        or []
    )

    products = []
    hydrate_details = os.getenv("WFM_FLYER_HYDRATE_DETAILS", "1").strip().lower() not in {"0", "false", "no"}
    max_promotions_raw = os.getenv("WFM_FLYER_MAX_PROMOTIONS", "").strip()
    max_promotions = int(max_promotions_raw) if max_promotions_raw.isdigit() else None

    if not hydrate_details:
        products = [build_flyer_display_product(p) for p in promotions]
    else:
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page(
                    viewport={"width": 1440, "height": 1800},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                    ),
                    locale="en-US",
                    timezone_id="America/New_York",
                )

                selected_promotions = promotions[:max_promotions] if max_promotions else promotions
                for index, p in enumerate(selected_promotions, start=1):
                    p = dict(p)
                    p["store_id"] = store_id
                    p["store_name"] = store_name
                    promotion_url = build_flyer_promotion_url(p, store_id=store_id)
                    detail_products = []
                    if promotion_url:
                        try:
                            print(
                                f"Flyer detail {index}/{len(selected_promotions)}: "
                                f"{p.get('productName', 'Unknown Product')} [{store_name or store_id}]"
                            )
                            page.goto(promotion_url, wait_until="domcontentloaded", timeout=90000)
                            try:
                                page.locator('a[data-csa-c-type="productTile"][href*="/grocery/product/"]').first.wait_for(
                                    timeout=12000
                                )
                            except Exception:
                                pass
                            load_more_clicks = expand_flyer_promotion_detail_page(page)
                            if load_more_clicks:
                                print(
                                    f"Flyer detail expanded {p.get('productName', 'Unknown Product')}: "
                                    f"clicked Load more {load_more_clicks} time(s)"
                                )
                            detail_products = parse_flyer_promotion_detail_products(page.content())
                        except Exception as exc:
                            print(
                                "Flyer detail scrape failed; falling back to display promo "
                                f"for {p.get('productName', 'Unknown Product')}: {exc}"
                            )

                    if detail_products:
                        for detail_index, detail_product in enumerate(detail_products, start=1):
                            products.append(
                                standardize_flyer_detail_product(
                                    p,
                                    detail_product,
                                    detail_index,
                                    len(detail_products),
                                )
                            )
                    else:
                        products.append(build_flyer_display_product(p))

                browser.close()
        except Exception as exc:
            print(f"Flyer detail hydration unavailable; using display promotions only: {exc}")
            products = [build_flyer_display_product(p) for p in promotions]

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
    subcategories = {value.lower() for value in parse_csv_arg("subcategory")}
    tags = {value.lower() for value in parse_csv_arg("tag")}
    brands = {value.lower() for value in parse_csv_arg("brand")}
    retailers = {value.lower() for value in parse_csv_arg("retailer")}
    sources = {value.lower() for value in parse_csv_arg("source")}
    store_ids = set(parse_csv_arg("store_id"))
    try:
        min_discount = float(request.args.get("min_discount", 0) or 0)
    except (TypeError, ValueError):
        min_discount = 0

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
        if subcategories and (product.get("subcategory") or "").lower() not in subcategories:
            continue
        if tags and not tags.intersection({tag.lower() for tag in product.get("tags") or []}):
            continue
        if brands and (product.get("brand") or "").lower() not in brands:
            continue
        if retailers and (product.get("retailer") or "").lower() not in retailers:
            continue
        if sources and not sources.intersection({source.lower() for source in (product.get("sources") or []) + (product.get("source_labels") or [])}):
            continue
        if min_discount and (product.get("discount_percent") or 0) < min_discount:
            continue
        if store_ids:
            available_store_ids = set(product.get("available_store_ids") or [])
            if available_store_ids and not store_ids.intersection(available_store_ids):
                continue

        filtered.append(product)

    return filtered


def sort_products_for_api(products):
    sort_mode = (request.args.get("sort") or "").strip().lower()
    ordered = list(products)
    if sort_mode == "discount":
        ordered.sort(key=lambda product: (-(product.get("discount_percent") or 0), normalize_text_key(product.get("name"))))
        return ordered
    if sort_mode == "price-asc":
        ordered.sort(key=lambda product: (parse_price_sort_value(product.get("prime_price") or product.get("current_price")), normalize_text_key(product.get("name"))))
        return ordered
    if sort_mode == "source-count":
        ordered.sort(key=lambda product: (-(product.get("source_count") or 0), -(product.get("discount_percent") or 0), normalize_text_key(product.get("name"))))
        return ordered
    return sort_products_for_display(ordered)


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


@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "storage": "supabase" if supabase_enabled() else "local",
        }
    )


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
    products = sort_products_for_api(filter_products_for_api(load_combined_products()))
    return jsonify({"products": products[:api_limit(default=120, maximum=1000)], "count": len(products)})


@app.route("/api/feed")
def api_feed():
    products = sort_products_for_api(filter_products_for_api(load_combined_products()))
    return jsonify({"products": products[:api_limit(default=5000, maximum=10000)], "count": len(products)})


@app.route("/api/product/<asin>")
def api_product(asin):
    asin = asin.strip()
    for product in load_combined_products():
        asins = set(product.get("asins") or [])
        if product.get("asin") == asin or asin in asins:
            return jsonify(product)
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/profile", methods=["GET", "POST", "OPTIONS"])
def api_profile():
    if request.method == "OPTIONS":
        return ("", 204)

    if request.method == "GET":
        device_id = (request.args.get("device_id") or "").strip()
        if not device_id:
            return jsonify({"error": "Missing device_id"}), 400
        return jsonify(
            {
                "profile": load_device_profile(device_id),
                "storage": "supabase" if supabase_enabled() else "local",
            }
        )

    payload = request.get_json(silent=True) or {}
    device_id = (payload.get("device_id") or "").strip()
    if not device_id:
        return jsonify({"error": "Missing device_id"}), 400

    profile = payload.get("profile") or {}
    saved_profile = save_device_profile(device_id, profile)
    return jsonify(
        {
            "ok": True,
            "profile": saved_profile,
            "storage": "supabase" if supabase_enabled() else "local",
        }
    )


@app.route("/api/fixes", methods=["GET", "POST", "OPTIONS"])
@app.route("/fixes-to-deploy", methods=["GET", "POST", "OPTIONS"])
@app.route("/api/category-feedback", methods=["GET", "POST", "OPTIONS"])
def api_fixes_to_deploy():
    if request.method == "OPTIONS":
        return ("", 204)

    if request.method == "GET":
        return jsonify(
            {
                "fixes": load_fixes_to_deploy(),
                "storage": "supabase" if supabase_enabled() else "local",
                "mode": "feedback-only",
            }
        )

    payload = request.get_json(silent=True) or {}
    kind = (payload.get("kind") or "").strip().lower()
    scope = (payload.get("scope") or "similar").strip().lower()
    product_key = (payload.get("product_key") or "").strip()
    signature = (payload.get("signature") or "").strip()
    fixes = load_fixes_to_deploy()

    if kind == "subcategory":
        subcategory = (payload.get("subcategory") or "").strip()
        category = (payload.get("category") or "").strip()
        active_options = active_taxonomy_subcategory_options()
        valid_pairs = {
            (current_category, current_subcategory)
            for current_category, subcategories in active_options.items()
            for current_subcategory in (subcategories or {}).keys()
        }
        if (category, subcategory) not in valid_pairs:
            return jsonify({"error": "Invalid subcategory"}), 400
        if scope not in {"item", "similar"}:
            return jsonify({"error": "Invalid scope"}), 400
        if scope == "item" and not product_key:
            return jsonify({"error": "Missing product key"}), 400
        if scope == "similar" and not signature:
            return jsonify({"error": "Missing signature"}), 400

        feedback_value = json.dumps({"category": category, "subcategory": subcategory}, ensure_ascii=False)
        if scope == "item":
            fixes["subcategory_overrides_by_key"][product_key] = feedback_value
        else:
            fixes["subcategory_overrides_by_signature"][signature] = feedback_value

        save_fixes_to_deploy(fixes)
        fix_id = f"subcategory:{scope}:{product_key or signature}"
        save_fix_to_supabase(
            fix_id=fix_id,
            fix_type="subcategory",
            scope=scope,
            product_key=product_key or None,
            signature=signature or None,
            retailer=(payload.get("retailer") or "").strip() or None,
            value=feedback_value,
            status="pending_feedback",
        )
        return jsonify(
            {
                "ok": True,
                "kind": kind,
                "scope": scope,
                "queued": True,
                "mode": "feedback-only",
                "category": category,
                "subcategory": subcategory,
            }
        )

    if kind == "brand":
        brand = clean_brand_display((payload.get("brand") or "").strip())
        if not brand:
            return jsonify({"error": "Invalid brand"}), 400
        if scope not in {"item", "similar"}:
            return jsonify({"error": "Invalid scope"}), 400
        if scope == "item" and not product_key:
            return jsonify({"error": "Missing product key"}), 400
        if scope == "similar" and not signature:
            return jsonify({"error": "Missing signature"}), 400

        if scope == "item":
            fixes["brand_overrides_by_key"][product_key] = brand
        else:
            fixes["brand_overrides_by_signature"][signature] = brand

        save_fixes_to_deploy(fixes)
        fix_id = f"brand:{scope}:{product_key or signature}"
        save_fix_to_supabase(
            fix_id=fix_id,
            fix_type="brand",
            scope=scope,
            product_key=product_key or None,
            signature=signature or None,
            value=brand,
            retailer=(payload.get("retailer") or "").strip() or None,
            status="pending_feedback",
        )
        return jsonify({"ok": True, "kind": kind, "scope": scope, "brand": brand, "queued": True, "mode": "feedback-only"})

    if kind == "gold_category":
        category = (payload.get("category") or "").strip()
        subcategory = (payload.get("subcategory") or "").strip()
        product = payload.get("product") if isinstance(payload.get("product"), dict) else {}
        active_options = active_taxonomy_subcategory_options()
        valid_pairs = {
            (current_category, current_subcategory)
            for current_category, subcategories in active_options.items()
            for current_subcategory in (subcategories or {}).keys()
        }
        if (category, subcategory) not in valid_pairs:
            return jsonify({"error": "Invalid category/subcategory"}), 400

        label = {
            "asin": (product.get("asin") or "").strip() or None,
            "url": (product.get("url") or "").strip() or None,
            "name": product.get("name"),
            "raw_name": product.get("raw_name") or product.get("name"),
            "brand": clean_brand_display((product.get("brand") or "").strip()) if product.get("brand") else "",
            "retailer": (product.get("retailer") or WHOLE_FOODS_RETAILER).strip(),
            "category": category,
            "subcategory": subcategory,
            "notes": "",
            "source": "site-category-fix",
        }
        label_id = gold_label_key(label)
        if not label_id:
            return jsonify({"error": "Missing product identity"}), 400

        gold_payload = load_gold_labels_payload()
        indexed = {}
        for existing in gold_payload.get("labels") or []:
            existing_id = gold_label_key(existing)
            if existing_id:
                indexed[existing_id] = existing
        indexed[label_id] = label
        gold_payload["labels"] = sorted(
            indexed.values(),
            key=lambda row: ((row.get("retailer") or ""), (row.get("name") or "")),
        )
        save_gold_labels_payload(gold_payload)
        return jsonify(
            {
                "ok": True,
                "kind": kind,
                "category": category,
                "subcategory": subcategory,
                "saved": True,
                "mode": "gold-label",
            }
        )

    if kind == "category_order":
        retailer = (payload.get("retailer") or "").strip()
        order = payload.get("order")
        device_id = (payload.get("device_id") or "").strip()
        if not retailer:
            return jsonify({"error": "Missing retailer"}), 400
        if not isinstance(order, list) or not all(isinstance(item, str) for item in order):
            return jsonify({"error": "Invalid order"}), 400

        if device_id:
            profile = load_device_profile(device_id) or {
                "selectedStoreIds": [],
                "likedKeys": [],
                "dislikedKeys": [],
                "categoryOrderByRetailer": {},
            }
            profile["categoryOrderByRetailer"] = dict(profile.get("categoryOrderByRetailer") or {})
            profile["categoryOrderByRetailer"][retailer] = order
            save_device_profile(device_id, profile)
            return jsonify({"ok": True, "kind": kind, "retailer": retailer, "order": order, "storage": "profile"})

        # Compatibility for older frontend builds that still post shelf order fixes
        # through /api/fixes before the profile-based path is deployed everywhere.
        return jsonify({"ok": True, "kind": kind, "retailer": retailer, "order": order, "storage": "legacy-noop"})

    return jsonify({"error": "Invalid fix kind"}), 400


@app.route("/", methods=["GET", "HEAD"])
def combined_products_home():
    if request.method == "HEAD":
        return ("", 204)

    if API_ONLY_MODE:
        return jsonify(
            {
                "ok": True,
                "service": "grocery-deals-api",
                "health": api_url("/health"),
                "feed": api_url("/api/feed"),
            }
        )

    products = sort_products_for_display(load_combined_products())
    taxonomy = load_active_taxonomy()
    deal_count = len(products)
    return render_template(
        "combined_products.html",
        products=products,
        deal_count=deal_count,
        available_stores=SUPPORTED_STORES,
        category_names=[category.get("name") for category in taxonomy.get("categories") or [] if category.get("name") != FAILED_CATEGORY],
        subcategory_options=taxonomy_to_options(taxonomy),
        category_order={},
        feedback_endpoint=api_url("/api/fixes"),
        profile_endpoint=api_url("/api/profile"),
        feed_endpoint=api_url("/api/feed"),
        page_subtitle="Browse Whole Foods, Target, and H Mart deals in one place.",
    )


if __name__ == "__main__":
    app.run(debug=True)
