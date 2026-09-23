"""Narrow source-backed taxonomy guards, applied after model/cache predictions."""
import re

def source_pair(product):
    name = (product.get('raw_name') or product.get('name') or '').lower()
    paths = ' '.join(product.get('source_categories') or []).lower()
    # Explicit product form outranks flavor, wellness claims, and model guesses.
    forms = [
        (r'\b(?:dish soap|dish liquid|dishwashing|dishwasher)\b', ('Household','Dishwashing')),
        (r'\b(?:laundry detergent|fabric softener)\b', ('Household','Laundry')),
        (r'\b(?:hand soap|bar soap|body wash|hand wash)\b', ('Beauty & Personal Care','Soap & Hand Wash')),

        (r'\b(?:diaper cream|diaper balm)\b', ('Baby','Baby Care')),
        (r'\b(?:body cream|body lotion|body scrub|hand cream)\b', ('Beauty & Personal Care','Body Care')),
        (r'\b(?:face cream|face care|moisturizer|facial serum)\b', ('Beauty & Personal Care','Skin Care')),
        (r'\b(?:shampoo|conditioner|hair color)\b', ('Beauty & Personal Care','Hair Care')),
        (r'\b(?:lip balm|lip gloss|lip butter)\b', ('Beauty & Personal Care','Lip Care')),
        (r'\b(?:superfood latte)\b', ('Beverages','Drink Mixes')),

        (r'\b(?:mac & cheese|mac and cheese|macaroni.*cheese|shells.*cheddar)\b', ('Pantry','Meal Kits & Sides')),
        (r'\b(?:dip mix|onion dip.*mix)\b', ('Pantry','Spices & Seasonings')),
        (r'\bfrozen vegetables\b', ('Frozen','Frozen Vegetables')),

        (r'\b(?:menstrual|tampon|period cup)\b', ('Beauty & Personal Care','Feminine Care')),
        (r'\b(?:teeth whitening|toothpaste|mouthwash)\b', ('Beauty & Personal Care','Oral Care')),
        (r'\b(?:disinfecting spray|disinfectant|surface cleaner)\b', ('Household','Cleaning Supplies')),
        (r'\b(?:beauty balm|jojoba oil|night cream)\b', ('Beauty & Personal Care','Skin Care')),
        (r'\b(?:dog|cat|pet) (?:food|treats?)\b', ('Household','Home Essentials')),
        (r'\b(?:seasoning|seasonings|seasoning mix)\b', ('Pantry','Spices & Seasonings')),
        (r'\b(?:pasta sauce|marinara|alfredo|vodka sauce)\b', ('Pantry','Pasta Sauces')),
        (r'\b(?:salad dressing|ranch.*dressing)\b', ('Pantry','Salad Dressings')),
        (r'\b(?:cocktail sauce|soy sauce)\b', ('Pantry','Condiments')),
        (r'\b(?:cereal|granola|coconola)\b', ('Pantry','Cereal & Breakfast')),
        (r'\b(?:protein shake|shake protein|protein drink)\b', ('Beverages','Functional Drinks')),
        (r'\b(?:sparkling adaptogen drink)\b', ('Beverages','Functional Drinks')),
        (r'\b(?:peanut|almond|cashew|hazelnut) (?:butter|spread)\b(?!.*\b(?:bar|cookie|candy|cups|chocolate cups)\b)', ('Pantry','Nut Butters & Spreads')),
        (r'\b(?:veggie burger|black bean burger)\b', ('Meat & Seafood','Meat Alternatives')),
        (r'\b(?:plant.based milk|oatmilk|almondmilk|soy milk|oat(?: & walnut)? milk|milk oat)\b', ('Dairy & Eggs','Plant-Based Milk')),
        (r'^(?!.*\b(?:hard|alcohol)\b).*\b(?:root beer|ginger beer|zero sugar soda)\b', ('Beverages','Soda')),
        (r'\b(?:wine vinegar|balsamic vinegar)\b', ('Pantry','Oils & Vinegars')),
        (r'\b(?:herbal tea|tea bags|yogi tea|energy tea)\b', ('Beverages','Tea' if not re.search(r'\b(?:fl|fz|ml)\b',name) else 'Ready-to-Drink Tea')),
        (r'\b(?:sauerkraut)\b', ('Pantry','Pickles & Fermented Pantry')),
        (r'\b(?:yogurt|skyr)\b(?!.*\b(?:covered|raisins|pretzels)\b)', ('Dairy & Eggs','Plant-Based Yogurt' if re.search(r'coconut|plant.based|dairy.free',name) else 'Yogurt')),
        (r'\b(?:pasta|bucatini)\b(?!.*\b(?:sauce|salad)\b)', ('Pantry','Pasta')),
    ]
    for pattern,pair in forms:
        if re.search(pattern,name):
            return pair, 'Explicit product form in retailer title'
    # Retailer breadcrumbs describe product form; flavor and brand tokens do not.
    if product.get('retailer') == 'H Mart':
        rules = [
            ('/beauty/makeup/', ('Beauty & Personal Care','Cosmetics')),
            ('/beauty/', ('Beauty & Personal Care','Skin Care')),
            
            ('/produce/fruit/', ('Produce','Fruits')),
            ('/dairy & egg/egg/', ('Dairy & Eggs','Eggs')),
            ('/dairy & egg/', ('Dairy & Eggs','Plant-Based Milk' if re.search(r'oat.?milk|almond|soy',name) else 'Yogurt' if 'yogurt' in name else 'Cheese' if 'cheese' in name else 'Butter & Margarine' if 'butter' in name else 'Milk')),
            ('/kimchi & sidedish & deli/tofu/', ('Prepared Foods','Tofu & Plant-Based Proteins')),
            ('/kimchi & sidedish & deli/ham & sausage/', ('Meat & Seafood','Hot Dogs & Franks')),
            ('/kimchi & sidedish & deli/', ('Prepared Foods','Kimchi & Banchan')),
            ('/rice & grain/', ('Pantry','Rice & Grains')),
            ('/paste & marinade & sauce/', ('Pantry','Marinades & Cooking Sauces')),
            ('/oil & seasoning & canned food/powders & sesame/', ('Pantry','Spices & Seasonings')),
            ('/oil & seasoning & canned food/oil/', ('Pantry','Oils & Vinegars')),
            ('/beverage & coffee & tea & honey/soy milk', ('Beverages','Functional Drinks')),
            ('/beverage & coffee & tea & honey/tea/', ('Beverages','Ready-to-Drink Tea' if 'fl' in name or 'ml' in name else 'Tea')),
            ('/beverage & coffee & tea & honey/coffee/', ('Beverages','Coffee' if 'sticks' in name else 'Ready-to-Drink Coffee')),
            ('/beverage & coffee & tea & honey/', ('Beverages','Juice')),
            ('/snacks & candy & nuts/bread & dessert/', ('Bakery','Pastries')),
            ('/snacks & candy & nuts/pies & cookies & biscuit/', ('Snacks','Cookies')),
            ('/snacks & candy & nuts/candy & chocolate/', ('Snacks','Candy & Chocolate')),
            ('/snacks & candy & nuts/', ('Snacks','Crackers' if 'cracker' in name else 'Chips')),
            ('/instant & quick food/cooked rice/', ('Prepared Foods','Prepared Meals')),
            ('/instant & quick food/', ('Prepared Foods','Dumplings & Quick Meals')),
            ('/ramen & noodle/', ('International','Asian Noodles & Dumplings')),
            ('/seaweed & dried produce/furikake/', ('Pantry','Spices & Seasonings')),
            ('/seaweed & dried produce/laver/', ('Snacks','Seaweed Snacks')),
            ('/seafood/', ('Meat & Seafood','Seafood')),
            ('/flour & baking/', ('Pantry','Flour & Meal')),
        ]
        for source, pair in rules:
            if source in paths:
                return pair, 'Retailer category: ' + source.strip('/')
    if product.get('offer_kind') == 'promotion':
        rules = [
            (r'^(?:organic )?(?:yellow peaches|honeycrisp apples|gala apples|organic green kiwis|seedless red or green grapes)',('Produce','Fruits')),
            (r'^(?:organic )?(?:heirloom tomatoes|cherry tomato|cauliflower|zucchini|corn,)',('Produce','Vegetables')),
            (r'cut cantaloupe',('Produce','Cut Fruit & Veg')),
            (r'wellness teas',('Beverages','Tea')),
            (r'ready.to.drink teas',('Beverages','Ready-to-Drink Tea')),
            (r'juices and lemonades',('Beverages','Juice')),
            (r'nut butters',('Pantry','Nut Butters & Spreads')),
            (r'^(?:hot )?sauces',('Pantry','Marinades & Cooking Sauces')),
            (r'almondmilk',('Dairy & Eggs','Plant-Based Milk')),
            (r'plant.based cheeses',('Dairy & Eggs','Dairy Alternatives')),
            (r'^select yogurts',('Dairy & Eggs','Yogurt')),
            (r'flatbread',('Prepared Foods','Prepared Meals')),
            (r'buffalo chicken wings|sous vide meals',('Prepared Foods','Ready-to-Eat Protein')),
            (r'beef burgers, meatballs or sausages',('Meat & Seafood','Sausage')),
        ]
        for pattern,pair in rules:
            if re.search(pattern,name):
                return pair, 'Explicit advertised product type'
    if 'lemonade' in name and not re.search(r'cookie|candy|mix|powder',name):
        return ('Beverages','Juice'), 'Lemonade product type'
    return None


def apply_source_rules(product):
    if product.get('ai_label_source') == 'gold':
        return product
    chosen = source_pair(product)
    if chosen:
        (category,subcategory),reason = chosen
        product.update(category=category,subcategory=subcategory,ai_category=category,ai_subcategory=subcategory,
                       category_confidence=0.99,ai_confidence=0.99,ai_label_source='source-rule',ai_reasoning=reason,
                       classification_status='classified')
    if product.get('brand_is_generic') or str(product.get('brand') or '').lower().startswith('fresh produce'):
        product.update(brand=None,source_brand=None,brand_source=None)
    return product
