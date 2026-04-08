import json
import re
import requests
import os
import math
from collections import Counter
from functools import lru_cache
from flask import Flask, jsonify, render_template, request, send_from_directory
from brand_ai import build_brand_family_map
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
SUBCATEGORY_AI_MODEL_FILE = os.path.join(BASE_DIR, "subcategory_ai_model.pkl")
SUBCATEGORY_AI_METADATA_FILE = os.path.join(BASE_DIR, "subcategory_ai_metadata.json")
SUBCATEGORY_AI_REPORT_FILE = os.path.join(BASE_DIR, "subcategory_ai_report.json")
SUBCATEGORY_AI_MIN_CONFIDENCE = 0.3

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
            "bread", "biscuit", "croissant", "cake", "muffin", "bagel", "cookie",
            "pie", "pastry", "brownie", "donut", "tortilla", "bun", "roll",
            "scone", "danish", "quiche",
        ],
        "medium": ["bakery", "baked", "pastry"],
        "weak": [],
        "exclude": [
            "ice cream cake", "pancake mix", "waffle mix", "chips", "chip", "tortilla chips",
            "rolled tortilla chips", "pretzel", "pretzels", "popcorn", "crisps", "puffs",
            "chickpea puffs", "cracker", "crackers",
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
            "protein bar", "fruit snacks", "cookies", "coconut chips",
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
        "Cookies & Biscuits": ["cookie", "biscuit", "shortbread"],
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
        "Cookies & Sweet Snacks": ["cookies", "cookie", "bites"],
        "Bars": ["granola bar", "protein bar", "snack bar", "bar"],
        "Nuts & Trail Mix": ["nuts", "trail mix", "almonds", "cashews", "pistachio"],
        "Jerky & Savory Protein Snacks": ["jerky", "meat stick", "protein crisps"],
        "Popcorn & Puffs": ["popcorn", "puffs", "cheese puffs"],
    },
    "Pantry": {
        "Pasta, Rice & Grains": ["pasta", "rice", "grain", "quinoa", "couscous", "farro"],
        "Sauces & Marinades": ["sauce", "marinade", "pasta sauce", "alfredo", "bolognese"],
        "Broth, Soup & Stock": ["broth", "soup", "stock"],
        "Dressings & Mayo": ["dressing", "mayo", "mayonnaise", "vinaigrette", "aioli"],
        "Dips & Spreads": ["hummus", "hommus", "dip", "dips", "guacamole", "queso"],
        "Nut Butters & Sweet Spreads": ["peanut butter", "almond butter", "cashew butter", "seed butter", "sunflower butter", "jam", "fruit spread", "preserves", "honey"],
        "Condiments": ["mustard", "ketchup", "hot sauce", "soy sauce", "condiment", "salsa"],
        "Oils & Vinegars": ["vinegar", "oil", "olive oil", "avocado oil"],
        "Baking Ingredients": ["flour", "baking soda", "baking powder", "cocoa powder", "vanilla extract"],
        "Spices & Seasonings": ["spice", "seasoning", "rub"],
        "Canned & Jarred Goods": ["jarred", "canned", "bruschetta", "canned tomato", "tomato paste", "paste tomato"],
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
        "Functional Beverages": ["kombucha", "functional beverage", "mushroom coffee", "adaptogen drink", "elixir"],
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
            "jerky", "granola bar", "protein bar", "matzo", "matzo-style",
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
            "paste tomato", "matzo ball",
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
        return clean_brand_display(cleaned_explicit_brand)

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


def clean_brand_display(brand):
    if not brand:
        return brand

    brand = re.sub(r"\s+", " ", brand).strip(" ,")
    canonical = canonical_brand_for_alias(brand)
    if canonical:
        brand = canonical

    tokens = re.split(r"(\s+)", brand)
    formatted = []
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
            if any(ch.isdigit() for ch in core) or "&" in core:
                formatted_core = core
            else:
                formatted_core = title_case_token(core.lower(), is_first=True)
        elif core.islower():
            formatted_core = title_case_token(core, is_first=True)
        elif any(ch.isupper() for ch in core[1:]) and any(ch.islower() for ch in core):
            formatted_core = core[0].upper() + core[1:]
        else:
            formatted_core = title_case_token(core, is_first=True)
        formatted.append(f"{prefix}{formatted_core}{suffix}")

    return "".join(formatted).strip()


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

    if brand and normalize_text_key(cleaned) == normalize_text_key(clean_brand_display(brand)):
        return smart_title_case(name)

    return smart_title_case(cleaned)


def normalize_brands_across_products(products):
    family_map = {}
    unique_brands = sorted(
        {product.get("brand") for product in products if product.get("brand")},
        key=lambda brand: (len(normalize_text_key(brand).split()), len(normalize_text_key(brand))),
    )

    for brand in unique_brands:
        brand_key = normalize_text_key(brand)
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

    ai_family_map, _ = build_brand_family_map(
        products,
        alias_map=BRAND_FAMILY_ALIASES,
        connectors=BRAND_CONNECTORS,
        generic_words=GENERIC_BRAND_WORDS,
        descriptor_starters=BRAND_DESCRIPTOR_STARTERS,
    )
    family_map.update(ai_family_map)

    for product in products:
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
        "categoryOrderByRetailer": profile.get("categoryOrderByRetailer") or {},
    }


def save_device_profile(device_id, profile):
    normalized = {
        "selectedStoreIds": profile.get("selectedStoreIds") or [],
        "likedKeys": profile.get("likedKeys") or [],
        "dislikedKeys": profile.get("dislikedKeys") or [],
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

    predictions = predict_subcategories(model, products) if model is not None else []

    for index, product in enumerate(products):
        product["previous_category"] = product.get("category")
        product["previous_subcategory"] = product.get("subcategory")

        existing_subcategory = product.get("subcategory")
        fixed_subcategory = (
            existing_subcategory in valid_subcategories
            and "queued fix" in (product.get("category_signals") or [])
        )
        prediction = predictions[index] if index < len(predictions) else None

        final_subcategory = existing_subcategory if existing_subcategory in valid_subcategories else None
        final_confidence = float(product.get("category_confidence") or 0)
        label_source = "heuristic-fallback"

        if fixed_subcategory:
            label_source = "fix"
            final_confidence = 1.0
        elif (
            prediction
            and prediction.get("subcategory") in valid_subcategories
            and (prediction.get("confidence") or 0) >= SUBCATEGORY_AI_MIN_CONFIDENCE
        ):
            final_subcategory = prediction["subcategory"]
            final_confidence = prediction.get("confidence") or 0
            label_source = "model"

        if not final_subcategory:
            fallback_haystack = build_classification_haystack(
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
            fallback_category = product.get("category") or "Pantry"
            final_subcategory = derive_subcategory(fallback_category, fallback_haystack)
            label_source = "heuristic-fallback"

        if final_subcategory in valid_subcategories:
            final_category = SUBCATEGORY_TO_CATEGORY[final_subcategory]
            product["category"] = final_category
            product["subcategory"] = final_subcategory

        if label_source == "model":
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
            product["ai_label_source"] = "fix"
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
        product["ai_label_source"] = product.get("ai_label_source") or "heuristic-fallback"
        product["ai_model_version"] = product.get("ai_model_version") or SUBCATEGORY_AI_MODEL_VERSION

    return products


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
    previous_brand_signature = None
    for _ in range(4):
        ordered = normalize_brands_across_products(ordered)
        ordered = apply_fixes_to_products(ordered)
        brand_signature = tuple(sorted({product.get("brand") or "" for product in ordered}))
        if brand_signature == previous_brand_signature:
            break
        previous_brand_signature = brand_signature
    ordered = apply_subcategory_ai(ordered)
    ordered.sort(
        key=lambda product: (
            -product.get("source_count", 0),
            normalize_text_key(product.get("name")),
        )
    )
    return ordered


def hydrate_combined_product_record(product, index=0):
    hydrated = dict(product)
    hydrated["asin"] = hydrated.get("asin")
    hydrated["asins"] = list(hydrated.get("asins") or ([hydrated["asin"]] if hydrated.get("asin") else []))
    hydrated["name"] = hydrated.get("name") or hydrated.get("raw_name") or f"Product {index + 1}"
    hydrated["raw_name"] = hydrated.get("raw_name") or hydrated["name"]
    hydrated["brand"] = hydrated.get("brand") or None
    hydrated["variation"] = hydrated.get("variation") or None
    hydrated["image"] = hydrated.get("image")
    hydrated["url"] = hydrated.get("url")
    hydrated["unit_price"] = hydrated.get("unit_price")
    hydrated["current_price"] = hydrated.get("current_price") or hydrated.get("sale_price")
    hydrated["basis_price"] = hydrated.get("basis_price")
    hydrated["prime_price"] = hydrated.get("prime_price")
    hydrated["discount"] = hydrated.get("discount")
    hydrated["retailer"] = hydrated.get("retailer") or "Whole Foods"
    hydrated["category"] = hydrated.get("category") or "Pantry"
    hydrated["subcategory"] = hydrated.get("subcategory")
    hydrated["category_confidence"] = float(hydrated.get("category_confidence") or 0)
    hydrated["category_signals"] = list(hydrated.get("category_signals") or [])
    hydrated["ai_subcategory"] = hydrated.get("ai_subcategory") or hydrated.get("subcategory")
    hydrated["ai_category"] = hydrated.get("ai_category") or hydrated.get("category")
    hydrated["ai_confidence"] = float(hydrated.get("ai_confidence") or hydrated.get("category_confidence") or 0)
    hydrated["ai_label_source"] = hydrated.get("ai_label_source") or "heuristic-fallback"
    hydrated["ai_model_version"] = hydrated.get("ai_model_version") or SUBCATEGORY_AI_MODEL_VERSION
    hydrated["sources"] = list(hydrated.get("sources") or [])
    hydrated["source_count"] = int(hydrated.get("source_count") or len(hydrated["sources"]))
    hydrated["tags"] = list(hydrated.get("tags") or [])
    hydrated["available_store_ids"] = list(hydrated.get("available_store_ids") or DEFAULT_STORE_IDS)
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
    products = [dict(product) for product in load_base_combined_products()]
    return apply_fixes_to_products(products)


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
    products = sort_products_for_display(filter_products_for_api(load_combined_products()))
    return jsonify({"products": products[:api_limit(default=120, maximum=1000)], "count": len(products)})


@app.route("/api/feed")
def api_feed():
    products = sort_products_for_display(filter_products_for_api(load_combined_products()))
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
        if subcategory not in SUBCATEGORY_TO_CATEGORY:
            return jsonify({"error": "Invalid subcategory"}), 400
        if scope not in {"item", "similar"}:
            return jsonify({"error": "Invalid scope"}), 400
        if scope == "item" and not product_key:
            return jsonify({"error": "Missing product key"}), 400
        if scope == "similar" and not signature:
            return jsonify({"error": "Missing signature"}), 400

        if scope == "item":
            fixes["subcategory_overrides_by_key"][product_key] = subcategory
        else:
            fixes["subcategory_overrides_by_signature"][signature] = subcategory

        save_fixes_to_deploy(fixes)
        fix_id = f"subcategory:{scope}:{product_key or signature}"
        save_fix_to_supabase(
            fix_id=fix_id,
            fix_type="subcategory",
            scope=scope,
            product_key=product_key or None,
            signature=signature or None,
            value=subcategory,
        )
        return jsonify({"ok": True, "kind": kind, "scope": scope, "subcategory": subcategory})

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
        )
        return jsonify({"ok": True, "kind": kind, "scope": scope, "brand": brand})

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
    deal_count = len(products)
    return render_template(
        "combined_products.html",
        products=products,
        deal_count=deal_count,
        available_stores=SUPPORTED_STORES,
        category_names=sorted(CATEGORY_PROFILES.keys()),
        subcategory_options=SUBCATEGORY_PROFILES,
        category_order={},
        feedback_endpoint=api_url("/api/fixes"),
        profile_endpoint=api_url("/api/profile"),
        feed_endpoint=api_url("/api/feed"),
        page_subtitle="Browse Whole Foods, Target, and H Mart deals in one place.",
    )


if __name__ == "__main__":
    app.run(debug=True)
