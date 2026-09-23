"""Translate specific retailer product types, retaining evidence for review.

Generic FOOD/GROCERY types deliberately do not supply a guessed category.
Title form checks run first because retailer types also contain mistakes.
"""
import re

GROUPS = {
 ('Beauty & Personal Care','Hair Care'): 'SHAMPOO CONDITIONER HAIR_COLORING_AGENT DRY_SHAMPOO HAIR_CLEANER_CONDITIONER HAIR_STYLING_AGENT',
 ('Beauty & Personal Care','Skin Care'): 'SKIN_MOISTURIZER SKIN_EXFOLIANT SKIN_CARE_AGENT SKIN_SERUM ASTRINGENT_SUBSTANCE SKIN_TREATMENT_MASK',
 ('Beauty & Personal Care','Body Care'): 'BODY_LUBRICANT BATHWATER_ADDITIVE PERSONAL_FRAGRANCE',
 ('Supplements & Wellness','Medicines & First Aid'): 'MEDICATION OTC_MEDICATION WOUND_DRESSING SKIN_PROTECTANT',
 ('Beauty & Personal Care','Soap & Hand Wash'): 'SKIN_CLEANING_AGENT HAND_SANITIZER SKIN_CLEANING_WIPE',
 ('Beauty & Personal Care','Lip Care'): 'LIP_BALM',
 ('Beauty & Personal Care','Oral Care'): 'DENTAL_FLOSS TOOTH_CLEANING_AGENT TOOTH_WHITENER TOOTHBRUSH MOUTHWASH',
 ('Beauty & Personal Care','Deodorant'): 'BODY_DEODORANT',
 ('Beauty & Personal Care','Sun Care'): 'SUNSCREEN',
 ('Beauty & Personal Care','Feminine Care'): 'MENSTRUAL_CUP SANITARY_NAPKIN',
 ('Beauty & Personal Care','Cosmetics'): 'NAIL_POLISH_REMOVER NAIL_POLISH EYELID_COLOR COSMETIC_POWDER MASCARA EYEBROW_COLOR LIP_COLOR',
 ('Beauty & Personal Care','Shaving & Grooming'): 'SHAVING_AGENT',
 ('Household','Cleaning Supplies'): 'CLEANING_AGENT BLEACH SURFACE_CLEANING_WIPE',
 ('Household','Dishwashing'): 'DISHWASHER_DETERGENT',
 ('Household','Laundry'): 'FABRIC_SOFTENER LAUNDRY_DETERGENT',
 ('Household','Paper Products'): 'PAPER_TOWEL TOILET_PAPER',
 ('Household','Kitchen Supplies'): 'FOOD_GRATER FOOD_SLICER JUICER SPOON WHISK_UTENSIL CUTTING_BOARD FLATWARE ROLLING_PIN DISHWARE_PLATE BLADED_FOOD_PEELER SHEET_PAN',
 ('Household','Home Essentials'): 'FRESH_CUT_FLOWERS_PLANTS CANDLE',
 ('Household','Food Storage'): 'FOOD_STORAGE_CONTAINER MEAL_HOLDER',
 ('Household','Trash Bags'): 'WASTE_BAG',
 ('Supplements & Wellness','Essential Oils'): 'ESSENTIAL_OIL',
 ('Supplements & Wellness','Herbal Supplements'): 'HERBAL_SUPPLEMENT',
 ('Supplements & Wellness','Minerals'): 'MINERAL_SUPPLEMENT',
 ('Supplements & Wellness','Vitamins'): 'VITAMIN',
 ('Supplements & Wellness','Protein & Collagen'): 'PROTEIN_SUPPLEMENT_POWDER',
 ('Beverages','Juice'): 'JUICE_AND_JUICE_DRINK',
 ('Beverages','Functional Drinks'): 'PROTEIN_DRINK MEAL_REPLACEMENT_BEVERAGE',
 ('Beverages','Drink Mixes'): 'FLAVORED_DRINK_CONCENTRATE',
 ('Pantry','Oils & Vinegars'): 'EDIBLE_OIL_VEGETABLE VINEGAR',
 ('Pantry','Rice & Grains'): 'CEREAL RICE_MIX',
 ('Pantry','Cereal & Breakfast'): 'BREAKFAST_CEREAL',
 ('Pantry','Nut Butters & Spreads'): 'NUT_BUTTER',
 ('Pantry','Salad Dressings'): 'SALAD_DRESSING',
 ('Pantry','Condiments'): 'CONDIMENT',
 ('Pantry','Pickles & Fermented Pantry'): 'PICKLE OLIVE',
 ('Pantry','Jams & Honey'): 'HONEY',
 ('Pantry','Sweeteners'): 'SUGAR SUGAR_SUBSTITUTE',
 ('Pantry','Spices & Seasonings'): 'SEASONING',
 ('Pantry','Beans & Legumes'): 'LEGUME',
 ('Pantry','Soup & Broth'): 'PACKAGED_SOUP_AND_STEW',
 ('Pantry','Flour & Meal'): 'FLOUR',
 ('Pantry','Baking Supplies'): 'THICKENING_AGENT BAKING_CHOCOLATE',
 ('Bakery','Bread'): 'BREAD',
 ('Bakery','Baking Mixes'): 'BAKING_MIX',
 ('Bakery','Pastries'): 'PASTRY',
 ('Bakery','Cakes & Cupcakes'): 'CAKE',
 ('Snacks','Candy & Chocolate'): 'CHOCOLATE_CANDY SUGAR_CANDY CANDY',
 ('Snacks','Chips'): 'SNACK_CHIP_AND_CRISP',
 ('Snacks','Crackers'): 'CRACKER',
 ('Snacks','Cookies'): 'COOKIE',
 ('Snacks','Nuts & Trail Mix'): 'SNACK_MIX',
 ('Snacks','Pretzels'): 'PRETZEL',
 ('Snacks','Jerky & Meat Snacks'): 'JERKY',
 ('Snacks','Popcorn'): 'POPCORN',
 ('Dairy & Eggs','Cheese'): 'DAIRY_BASED_CHEESE',
 ('Dairy & Eggs','Cream & Creamers'): 'DAIRY_BASED_CREAM NON_DAIRY_CREAM',
 ('Dairy & Eggs','Yogurt'): 'DAIRY_BASED_YOGURT',
 ('Dairy & Eggs','Butter & Margarine'): 'DAIRY_BASED_BUTTER',
 ('Dairy & Eggs','Plant-Based Milk'): 'MILK_SUBSTITUTE',
 ('Dairy & Eggs','Plant-Based Yogurt'): 'NON_DAIRY_YOGURT',
 ('Dairy & Eggs','Dairy Alternatives'): 'NON_DAIRY_CHEESE VEGETARIAN_EGG_SUBSTITUTE',
 ('Frozen','Ice Cream'): 'DAIRY_BASED_ICE_CREAM',
 ('Meat & Seafood','Meat Alternatives'): 'MEAT_ALTERNATIVE',
 ('Prepared Foods','Tofu & Plant-Based Proteins'): 'TOFU',
 ('Alcohol','Wine'): 'WINE', ('Alcohol','Beer'): 'BEER', ('Alcohol','Spirits'): 'SPIRITS',
 ('Baby','Formula'): 'BABY_FORMULA',
}
TYPE_PAIRS = {kind: pair for pair, kinds in GROUPS.items() for kind in kinds.split()}


def retailer_pair(product):
    kind = product.get('retailer_product_type')
    if not kind or not product.get('metadata_observed_at'):
        return None
    name = (product.get('raw_name') or product.get('name') or '').lower().split('|')[0]
    pair = TYPE_PAIRS.get(kind)
    ingredients = (product.get('retailer_ingredients') or '').lower()
    # Manufacturer evidence: these named ranges are canned/frozen, not fresh.
    # https://store.edwardandsons.com/collections/vegetables
    # https://www.edwardandsons.org/docs/OnlineSells/USFS25.pdf
    if name.startswith('native forest') and kind in ('FRUIT','VEGETABLE','CULINARY_MUSHROOM'):
        return ('Pantry','Canned Fruit' if kind == 'FRUIT' else 'Canned Vegetables'), 'Manufacturer canned fruit/vegetable range: Native Forest'
    # https://www.stahlbush.com/introducing-stahlbush-island-farms-new-frozen-organic-blends/
    if 'stahlbush' in name and re.search(r'(farmhouse|mediterranean) blend',name):
        return ('Frozen','Frozen Vegetables'), 'Manufacturer frozen vegetable blend: Stahlbush'
    if kind == 'HEALTH_PERSONAL_CARE' and 'tablets' in name and 'hpus' in ingredients:
        return ('Supplements & Wellness','Medicines & First Aid'), 'Retailer tablet form and homeopathic ingredient label'
    if kind == 'FOOD' and ingredients.strip() == 'organic pumpkin puree':
        return ('Pantry','Canned Vegetables'), 'Retailer pumpkin puree identity and ingredient label'
    if kind == 'FOOD' and 'bean' in name and 'chili' in name:
        return ('Pantry','Soup & Broth'), 'Retailer bean chili product title'
    if kind in ('MEAT','POULTRY','PET_FOOD') and re.search(r'\b(nuggets|tenders)\b',name) and 'chicken breast' in ingredients.replace(',', ''):
        return ('Meat & Seafood','Chicken'), 'Retailer chicken ingredient and breaded product form'

    description = (product.get('retailer_description') or '').lower().strip()
    if 'alden' in name and 'yogurt bars' in name:
        return ('Frozen','Frozen Desserts'), 'Frozen yogurt bar product form'
    if kind == 'FOOD' and 'kite hill' in name and 'yogurt' in name:
        return ('Dairy & Eggs','Plant-Based Yogurt'), 'Retailer plant-based yogurt identity'
    if kind == 'PREPARED_AND_PRESERVED_FOOD' and re.search(r'chicken|beef|turkey',name):
        return ('Prepared Foods','Ready-to-Eat Protein'), 'Retailer prepared protein product type'
    if kind == 'FOOD' and description == 'grocery frozen':
        pair = ('Frozen','Frozen Meals')
    if kind == 'DRINK_FLAVORED':
        if re.search(r'\b(nuun|tablets?)\b',name): pair = ('Supplements & Wellness','Hydration Supplements')
        elif re.search(r'\b(hydration|electrolyte)\b',name): pair = ('Beverages','Sports Drinks')
        elif re.search(r'\b(fl|fz|ml)\b',name): pair = ('Beverages','Functional Drinks')
    if kind == 'FOOD' and 'impossible' in name: pair = ('Meat & Seafood','Meat Alternatives')
    if kind == 'FOOD' and 'violife' in name and '100% vegan' in description: pair = ('Dairy & Eggs','Dairy Alternatives')

    if kind in ('FRUIT','VEGETABLE','CULINARY_MUSHROOM'):
        if 'frozen' in name:
            pair = ('Frozen','Frozen Fruit' if kind == 'FRUIT' else 'Frozen Vegetables')
        elif re.search(r'\b(canned|cans?|jarred)\b', name):
            pair = ('Pantry','Canned Fruit' if kind == 'FRUIT' else 'Canned Vegetables')
        else:
            pair = ('Produce','Fruits' if kind == 'FRUIT' else 'Mushrooms' if kind == 'CULINARY_MUSHROOM' else 'Vegetables')
    elif kind == 'FRUIT_SNACK':
        pair = ('Pantry','Canned Fruit') if re.search(r'\b(canned|cans?|fruit cocktail)\b',name) or 'native forest' in name else ('Snacks','Fruit Snacks')
    elif kind == 'SNACK_FOOD_BAR':
        pair = ('Frozen','Frozen Desserts') if re.search(r'frozen|ice cream',name) else ('Snacks','Protein Bars' if 'protein' in name else 'Snack Bars')
    elif kind == 'SAUCE': pair = ('Pantry','Marinades & Cooking Sauces')
    elif kind == 'NUT_AND_SEED': pair = ('Produce','Nuts & Seeds')
    elif kind == 'WATER': pair = ('Beverages','Sparkling Water' if re.search('sparkling|seltzer',name) else 'Coconut Water' if 'coconut' in name else 'Water')
    elif kind == 'TEA': pair = ('Beverages','Ready-to-Drink Tea' if re.search(r'\b(fl|fz|ml)\b',name) else 'Tea')
    elif kind == 'COFFEE': pair = ('Beverages','Coffee Pods & K-Cups' if re.search('pods|k-cup',name) else 'Ready-to-Drink Coffee' if re.search(r'\b(fl|fz|ml)\b',name) else 'Coffee Beans & Grounds')
    elif kind == 'NOODLE': pair = ('Pantry','Pasta')
    elif kind == 'FISH': pair = ('Pantry','Canned Fish & Meat') if re.search(r'\b(can|canned|tinned)\b',name) else ('Meat & Seafood','Smoked Seafood' if 'smoked' in name else 'Fish Fillets')
    elif kind in ('SEAFOOD','SHELLFISH'): pair = ('Meat & Seafood','Shrimp' if re.search('shrimp|prawn',name) else 'Crab & Lobster' if re.search('crab|lobster',name) else 'Seafood')
    elif kind in ('MEAT','POULTRY'):
        for pattern, sub in [('sausage','Sausage'),('bacon','Bacon'),('frank|hot dog','Hot Dogs & Franks'),('chicken','Chicken'),('turkey','Turkey'),('beef','Beef'),('pork','Pork'),('lamb','Lamb')]:
            if re.search(pattern,name): pair = ('Meat & Seafood',sub); break
    elif kind in ('NUTRITIONAL_SUPPLEMENT','DIETARY_SUPPLEMENTS'):
        for pattern,sub in [('probiotic','Probiotics'),('collagen|protein','Protein & Collagen'),('omega|fish oil','Omega & Fish Oil'),('multivitamin','Multivitamins'),('vitamin','Vitamins'),('magnesium|zinc|calcium|iron','Minerals'),('melatonin|sleep','Sleep Support'),('digest|enzyme','Digestive Support'),('immune','Immune Support'),('electrolyte','Hydration Supplements')]:
            if re.search(r'\b(?:'+pattern+r')\b',name): pair=('Supplements & Wellness',sub); break
    if kind in ('NUTRITIONAL_SUPPLEMENT','DIETARY_SUPPLEMENTS') and not pair:
        pair = ('Supplements & Wellness','Dietary Supplements')
    if pair:
        return pair, 'Retailer product type: ' + kind
    return None
