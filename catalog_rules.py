"""Narrow source-backed taxonomy guards, applied after model/cache predictions."""
import re
from retailer_categories import retailer_pair

def source_pair(product):
    name = (product.get('raw_name') or product.get('name') or '').lower()
    paths = ' '.join(product.get('source_categories') or []).lower()
    if product.get('retailer_product_type') == 'PREPARED_AND_PRESERVED_FOOD' or ('alden' in name and 'yogurt bars' in name) or ('kite hill' in name and 'yogurt' in name):
        specific = retailer_pair(product)
        if specific:
            return specific
    # Specific packaged forms outrank ingredient/flavor phrases such as oat milk.
    if product.get('retailer_product_type') in {'CHOCOLATE_CANDY','SUGAR_CANDY','CANDY','COOKIE','PROTEIN_SUPPLEMENT_POWDER','NON_DAIRY_YOGURT','NON_DAIRY_CHEESE'}:
        specific = retailer_pair(product)
        if specific:
            return specific
    # Explicit product form outranks flavor, wellness claims, and model guesses.
    forms = [
        (r'\bsparkling hop water\b', ('Beverages','Sparkling Water')),
        (r'\b(?:tempeh)\b', ('Prepared Foods','Tofu & Plant-Based Proteins')),
        (r'\b(?:kombucha)\b', ('Beverages','Kombucha')),
        (r'\b(?:soda)\b(?!.*\b(?:baking|bread)\b)', ('Beverages','Soda')),
        (r'\b(?:non.alcoholic|non alcoholic|non.alc).*\b(?:beer|lager|ipa|wine)\b', ('Alcohol','Non-Alcoholic Beer & Wine')),
        (r'^(?!.*\b(?:powder|mix|bar|cookie)\b).*\blatte\b.*\b(?:fl|fz|ml)\b', ('Beverages','Ready-to-Drink Coffee')),

        (r'\btrail mix\b', ('Snacks','Nuts & Trail Mix')),
        (r'\bdried (?:shiitake|shitake|mushrooms?)\b', ('Pantry','Dried Vegetables & Mushrooms')),

        (r'\bhard seltzer\b', ('Alcohol','Hard Seltzer')),
        (r'\b(?:vitamins & minerals)\b', ('Supplements & Wellness','Multivitamins')),
        (r'\b(?:sparkling.*mixer|non.alcoholic.*cocktails)\b', ('Beverages','Syrups & Mixers')),
        (r'\b(?:wellness shot|goodbelly.*shot)\b', ('Supplements & Wellness','Wellness Shots')),
        (r'\b(?:food storage container|duraglass.*container)\b', ('Household','Food Storage')),
        (r'\b(?:croutons)\b', ('Pantry','Meal Kits & Sides')),
        (r'\b(?:taco shells)\b', ('Bakery','Tortillas & Wraps')),
        (r'\b(?:pumpkin pie)\b', ('Bakery','Pies & Tarts')),
        (r'\b(?:shredded coconut)\b', ('Pantry','Baking Supplies')),
        (r'\b(?:three bean.*salad|salad three bean)\b', ('Prepared Foods','Salads & Sides')),
        (r'\b(?:hemp seed oil|mct oil)\b', ('Pantry','Oils & Vinegars')),
        (r'^peter rabbit organics.*puree', ('Baby','Baby Food')),
        (r'\b(?:breakfast biscuits)\b', ('Snacks','Cookies')),

        (r'\b(?:salami|guanciale|prosciutto)\b', ('Meat & Seafood','Deli Meats')),
        (r'\b(?:protein powder|whey protein|protein whey|collagen peptides|creatine)\b', ('Supplements & Wellness','Protein & Collagen')),
        (r'\b(?:yogurt bar|yogurt.*melts|smoothie melts)\b', ('Snacks','Snack Bars' if 'bar' in name else 'Fruit Snacks')),
        (r'\b(?:water chestnuts)\b', ('Pantry','Canned Vegetables')),
        (r'\b(?:fruit spread|preserves|fig.*spread|cherry spread)\b', ('Pantry','Jams & Honey')),
        (r'\b(?:pesto|tomato basil sauce)\b', ('Pantry','Pasta Sauces')),
        (r'\b(?:cooking sauce|curry sauce|tonkatsu)\b', ('Pantry','Marinades & Cooking Sauces')),
        (r'\b(?:dipping sauce|aioli)\b', ('Pantry','Condiments')),
        (r'\b(?:hot sauce|sauce hot|sauce buffalo)\b', ('Pantry','Hot Sauce & BBQ Sauce')),
        (r'\b(?:gravy mix|season mix)\b', ('Pantry','Spices & Seasonings')),
        (r'\b(?:hydration drink|electrolyte refresher)\b', ('Beverages','Sports Drinks')),
        (r'\b(?:overnight oats)\b', ('Pantry','Cereal & Breakfast')),
        (r'\b(?:chia seeds)\b', ('Produce','Nuts & Seeds')),
        (r'\b(?:labneh)\b', ('Dairy & Eggs','Yogurt')),
        (r'\b(?:plant.based meatballs|plant.based.*(?:steak|cubes)|steak plant.based)\b', ('Meat & Seafood','Meat Alternatives')),
        (r'\b(?:protein bowl|vegetable curry meal)\b', ('Prepared Foods','Prepared Meals')),
        (r'\b(?:fruit jerky)\b', ('Snacks','Fruit Snacks')),
        (r'\b(?:superfruit punch)\b', ('Beverages','Juice')),
        (r'\b(?:guacamole)\b', ('Produce','Fresh Salsa & Dips')),
        (r'\b(?:turkey meatloaf)\b', ('Prepared Foods','Ready-to-Eat Protein')),
        (r'\b(?:yerba mate)\b', ('Beverages','Tea' if 'tea bags' in name else 'Ready-to-Drink Tea')),
        (r'\b(?:non.alcoholic.*(?:cocktail|paloma)|non.alc aperitif)\b', ('Beverages','Syrups & Mixers')),

        (r'\b(?:frozen yogurt|ice cream)\b', ('Frozen','Frozen Desserts')),
        (r'^(?!.*\b(?:shampoo|conditioner|lotion|body|skin|soap)\b).*\b(?:olive oil|avocado oil|evoo)\b', ('Pantry','Oils & Vinegars')),
        (r'\bchocolate syrup\b', ('Pantry','Sweeteners')),
        (r'\bkefir\b', ('Dairy & Eggs','Yogurt')),
        (r'\b(?:cold smoked salmon|smoked salmon)\b', ('Meat & Seafood','Smoked Seafood')),
        (r'^tea black\b', ('Beverages','Ready-to-Drink Tea')),
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
    sourced = retailer_pair(product)
    if sourced:
        return sourced
    # Retailer breadcrumbs describe product form; flavor and brand tokens do not.
    if product.get('retailer') == 'H Mart':
        rules = [
            ('/health/', ('Supplements & Wellness','Dietary Supplements')),
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
            (r'hummus and dairy dips',('Produce','Fresh Salsa & Dips')),
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
    if chosen:
        product.pop('failed_from_category', None)
        product.pop('failed_from_subcategory', None)
    title = (product.get('raw_name') or product.get('name') or '').lower()
    if title.startswith("kevin's natural foods"):
        product['brand'] = "Kevin's Natural Foods"
        product['brand_source'] = 'retailer-title'
    if product.get('metadata_observed_at') and product.get('brand_source') == 'derived' and not product.get('source_brand'):
        # Missing retailer brand is not permission to invent one from adjectives.
        product['brand'] = None
        product['brand_source'] = None
    if product.get('brand_is_generic') or str(product.get('brand') or '').lower().startswith('fresh produce'):
        product.update(brand=None,source_brand=None,brand_source=None)
    return product
