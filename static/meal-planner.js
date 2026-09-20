/* Shared, dependency-free planner: used by the static site and Node tests. */
(function (root) {
  'use strict';
  const ingredients = {
    oats: ['Rolled oats', 'g', /\b(rolled oats|old fashioned oats|quick oats)\b/, 'Pantry'],
    milk: ['Milk', 'ml', /\b(milk)\b/, 'Dairy & Eggs'],
    banana: ['Bananas', 'each', /\bbananas?\b/, 'Produce'],
    yogurt: ['Plain yogurt', 'g', /\b(plain|greek)\b.*\byogurt\b|\byogurt\b.*\bplain\b/, 'Dairy & Eggs'],
    berries: ['Berries', 'g', /\b(blueberries|strawberries|raspberries|mixed berries)\b/, 'Produce'],
    eggs: ['Eggs', 'each', /\beggs\b/, 'Dairy & Eggs'],
    bread: ['Sliced bread', 'g', /\b(bread|sourdough loaf)\b/, 'Bakery'],
    spinach: ['Spinach', 'g', /\bspinach\b/, 'Produce'],
    chickpeas: ['Canned chickpeas (drained)', 'g', /\b(chickpeas|garbanzo beans)\b/, 'Pantry'],
    beans: ['Canned black beans (drained)', 'g', /\bblack beans\b/, 'Pantry'],
    rice: ['Dry rice', 'g', /\b(brown rice|white rice|jasmine rice|basmati rice)\b/, 'Pantry'],
    pasta: ['Dry pasta', 'g', /\b(penne|spaghetti|fusilli|rotini)\b/, 'Pantry'],
    tomatoes: ['Canned tomatoes', 'g', /\b(diced|crushed|whole peeled) tomatoes\b/, 'Pantry'],
    broccoli: ['Broccoli', 'g', /\bbroccoli\b/, 'Produce'],
    pepper: ['Bell peppers', 'each', /\b(bell peppers?|sweet peppers?)\b/, 'Produce'],
    onion: ['Onions', 'each', /\b(yellow onions?|red onions?|sweet onions?|white onions?)\b/, 'Produce'],
    potato: ['Potatoes', 'g', /\b(potatoes|russet potato|gold potato)\b/, 'Produce'],
    carrot: ['Carrots', 'g', /\bcarrots?\b/, 'Produce'],
    tofu: ['Firm tofu', 'g', /\b(firm|extra firm) tofu\b|\btofu\b.*\bfirm\b/, 'Refrigerated'],
    chicken: ['Raw boneless chicken breast', 'g', /\bchicken breasts?\b/, 'Meat & Seafood'],
    salmon: ['Raw salmon fillets', 'g', /\bsalmon\b/, 'Meat & Seafood'],
    tortillas: ['Tortillas', 'each', /\btortillas\b/, 'Bakery'],
    cheese: ['Cheddar cheese', 'g', /\bcheddar\b/, 'Dairy & Eggs'],
    oil: ['Olive oil', 'ml', /\bolive oil\b/, 'Pantry'],
    salt: ['Salt', 'g', /\bsea salt\b/, 'Pantry'],
    seasoning: ['Ground cumin', 'g', /\bground cumin\b/, 'Pantry'],
  };
  // Quantities are per person; dry grains and drained canned legumes are explicit.
  const recipes = [
    ['Banana oatmeal', 'breakfast', {oats:60,milk:180,banana:1}, 'Simmer oats with milk until soft, stirring often. Slice the banana over the top.'],
    ['Berry yogurt bowls', 'breakfast', {yogurt:200,berries:100,oats:30}, 'Toast the oats in a dry pan for 3–4 minutes. Spoon yogurt into bowls and top with berries and oats.'],
    ['Spinach eggs on toast', 'breakfast', {eggs:2,bread:70,spinach:50,oil:5,salt:0.5}, 'Wilt spinach in oil. Add beaten eggs and salt; scramble until set. Serve on toasted bread.'],
    ['Banana yogurt toast', 'breakfast', {bread:70,yogurt:150,banana:1}, 'Toast bread, spread with yogurt, and top with sliced banana. Serve remaining yogurt alongside.'],
    ['Chickpea spinach wraps', 'lunch', {chickpeas:160,spinach:50,tortillas:2,yogurt:40,seasoning:1,salt:0.5}, 'Drain and rinse chickpeas. Mash with yogurt, cumin, and salt. Fill tortillas with chickpeas and spinach.'],
    ['Black bean rice bowls', 'lunch', {beans:160,rice:70,pepper:0.5,onion:0.25,oil:5,seasoning:1,salt:0.5}, 'Cook rice according to the package. Sauté diced onion and pepper in oil; add drained beans, cumin, and salt. Serve over rice.'],
    ['Broccoli cheddar baked potatoes', 'lunch', {potato:300,broccoli:150,cheese:40,oil:5,salt:0.5}, 'Pierce potatoes and bake at 425°F until tender, about 45–60 minutes. Steam broccoli. Split potatoes and top with broccoli, cheese, oil, and salt.'],
    ['Chickpea tomato soup', 'lunch', {chickpeas:160,tomatoes:200,carrot:100,onion:0.25,bread:70,oil:5,salt:0.5}, 'Sauté diced onion and carrot in oil. Add tomatoes, drained chickpeas, salt, and enough water to loosen. Simmer until carrots are tender; serve with bread.'],
    ['Egg and spinach wraps', 'lunch', {eggs:2,spinach:70,tortillas:2,cheese:30,oil:5,salt:0.5}, 'Wilt spinach in oil, add beaten eggs and salt, and scramble until set. Divide between tortillas and add cheese.'],
    ['Tomato chickpea pasta', 'dinner', {pasta:90,chickpeas:130,tomatoes:180,spinach:60,onion:0.25,oil:10,salt:0.5}, 'Cook pasta according to the package. Sauté onion in oil; add tomatoes, drained chickpeas, and salt. Simmer 10 minutes, wilt in spinach, then toss with pasta.'],
    ['Roasted tofu and broccoli rice', 'dinner', {tofu:180,broccoli:180,rice:75,oil:10,seasoning:1,salt:0.5}, 'Cook rice according to the package. Drain and cube tofu; toss with broccoli, oil, cumin, and salt. Roast at 425°F for 25–30 minutes, turning once, and serve over rice.'],
    ['Black bean pepper tacos', 'dinner', {beans:180,pepper:1,onion:0.25,tortillas:3,cheese:30,oil:5,seasoning:1,salt:0.5}, 'Sauté sliced pepper and onion in oil. Add drained beans, cumin, and salt. Heat through and serve in warm tortillas with cheese.'],
    ['Chickpea potato skillet', 'dinner', {chickpeas:180,potato:220,spinach:70,onion:0.25,oil:10,seasoning:1,salt:0.5}, 'Dice potatoes small and sauté in oil with onion. Add a splash of water, cover, and cook until tender. Add drained chickpeas, cumin, salt, and spinach; heat through.'],
    ['Broccoli and egg fried rice', 'dinner', {rice:80,eggs:2,broccoli:150,carrot:80,oil:10,salt:0.5}, 'Cook rice according to the package. Sauté chopped broccoli and carrot in oil until tender. Add beaten eggs and cook until set, then stir in rice and salt.'],
    ['Tomato tofu stew', 'dinner', {tofu:180,tomatoes:200,carrot:100,rice:75,onion:0.25,oil:10,seasoning:1,salt:0.5}, 'Cook rice according to the package. Sauté onion and carrot in oil. Add tomatoes, cubed tofu, cumin, salt, and a splash of water. Simmer until carrots are tender and serve over rice.'],
    ['Cheddar bean quesadillas', 'dinner', {beans:180,tortillas:3,cheese:60,pepper:0.5,spinach:60,oil:5,seasoning:1}, 'Sauté pepper and spinach in oil. Mash drained beans with cumin. Fill tortillas with beans, vegetables, and cheese; fold and toast in a skillet until hot and crisp.'],
    ['Sheet-pan chicken and potatoes', 'dinner', {chicken:180,potato:250,carrot:150,oil:10,seasoning:1,salt:0.5}, 'Cut potatoes and carrots into small pieces; toss with oil, cumin, and salt. Roast at 425°F for 15 minutes, add chicken, and continue until cooked through. Use a food thermometer and follow the chicken package’s safe-cooking instructions.'],
    ['Salmon broccoli rice bowls', 'dinner', {salmon:180,broccoli:180,rice:75,oil:10,salt:0.5}, 'Cook rice according to the package. Toss broccoli with oil and salt; roast at 425°F for 10 minutes. Add salmon and roast until cooked through, following the package’s safe-cooking instructions. Serve over rice.'],
  ].map(([name,slot,items,method],id) => ({id,name,slot,items,method,vegetarian:!items.chicken && !items.salmon}));
  const processed = /\b(chips?|crackers?|cookies?|bars?|powder|supplement|baby food|dog|cat|shampoo|soap|candy|juice|smoothie|ice cream|pizza|soup|sauce|seasoned|flavored|flavoured|breaded|nuggets?|tenders|marinated|smoked|roasted|grilled|cooked|sous vide|kabob|sausage|jerky|salad|shells|fries|burger|meatballs|poppers|balls|corn|chocolate|chocolat|cacao|candle|cleanser|cleansing|serum|wash|conditioner|tea|shake|hummus|spread|cocoa|mac|macaroni|happy baby|puree|pouch|popcorn|kombucha|ravioli|tortellini|cheezmos|puffs?|crisps?|snack|pasta meal|ready to eat)\b/i;
  function money(value) {
    const match = String(value || '').trim().match(/^\$?(\d+(?:\.\d{1,2})?)(?=\s|\/|$)/);
    return match ? Number(match[1]) : null;
  }
  function matches(id, product) {
    const name = String(product.name || '').toLowerCase();
    if (!ingredients[id][2].test(name) || processed.test(name)) return false;
    if (['banana','berries','spinach','broccoli','pepper','onion','potato','carrot'].includes(id) && !['Produce','Frozen'].includes(product.category)) return false;
    if (id === 'banana' && !/^(?:organic\s+)?bananas?(?:\s+organic)?(?:,?\s+[\d.]+\s*(?:lb|oz|each|count))?$/.test(name)) return false;
    if (id === 'cheese' && !/cheddar(?:\s+(?:cheese|slices?|shreds?|block|bar|cuts|cubes|aged|sticks)|,|$)/.test(name)) return false;
    if (id === 'salt' && !/fine|coarse|ground|canister/.test(name)) return false;
    if (id === 'salt' && !/sea salt(?: canister)?(?:,?\s+[\d.]+\s*(?:oz|ounce|ounces|lb))?$/.test(name)) return false;
    if (id === 'milk' && !/milk(?:,?\s+(?:original\s+)?[\d.]+\s*(?:fl|oz|ounce|gal)|$)/.test(name)) return false;
    if (id === 'chicken' && !/boneless/.test(name)) return false;
    if (id === 'salmon' && !/fillet|filet/.test(name)) return false;
    if (['chickpeas','beans'].includes(id) && !/canned|\b15(?:\.\d+)?\s*(?:oz|ounce)/.test(name)) return false;
    if (['Household','Beauty & Personal Care','Supplements & Wellness','Alcohol','Baby','Snacks','Prepared Foods'].includes(product.category)) return false;
    const rejects = {
      milk: /coconut|almond|oat|soy|chocolate|condensed|evaporated|buttermilk|yogurt|kefir|cheese|cream/,
      eggs: /pasta|noodle|nest|egg white|liquid|plant|vegan/,
      banana: /dried|freeze|bread|puree|yogurt|pudding/,
      berries: /dried|freeze dried|preserve|jam/,
      bread: /crumb|garlic|banana|pumpkin|mix/,
      chickpeas: /dry|dried|flour|pasta|hummus/,
      beans: /dry|dried|refried|rice|noodle/,
      rice: /pasta|noodle|flour|vinegar|paste|quinoa|beans|mix|pouch|instant/,
      pasta: /chicken|beef|pork|shrimp|lobster|stuffed/,
      spinach: /dip|pasta|tortilla|wrap/,
      broccoli: /cheddar|cheese|rice|slaw/,
      potato: /sweet|mashed|flakes/,
      chicken: /bone.in|skin.on|stuffed|cutlet|deli|shredded/,
      salmon: /burger|cake|spread|dip|lox|canned/,
      cheese: /vegan|plant|cracker|spread|soup|cheezmos|pasta|macaroni/,
      oil: /spray|blend|infused/,
      salt: /chocolate|caramel|nuts|butter/,
    };
    return !rejects[id]?.test(name);
  }
  function analyze(products, options) {
    const now = options.today || new Date().toISOString().slice(0,10);
    const eligible = products.flatMap(original => {
      if (options.retailer !== 'All' && original.retailer !== options.retailer) return [];
      let p = original;
      if (options.store && p.retailer === 'Whole Foods') {
        const offer = (p.store_offers || []).find(o => String(o.store_id) === options.store);
        if (!offer) return []; // Never substitute another location's price.
        p = {...p, current_price:null, sale_price:null, prime_price:null, basis_price:null, ...offer};
      }
      if (p.expires) {
        const expiry = Date.parse(p.expires);
        if (Number.isFinite(expiry) && expiry < Date.parse(now)) return [];
      }
      const priceText = options.prime ? (p.prime_price || p.current_price || p.sale_price) : (p.current_price || p.sale_price);
      const price = money(priceText), regular = money(p.basis_price);
      if (!(price > 0 && regular > price)) return [];
      return [{...p,priceText,price,isPrime:Boolean(options.prime && p.prime_price && p.retailer === 'Whole Foods'),discount:Math.round((1-price/regular)*100)}];
    });
    const deals = {};
    for (const id of Object.keys(ingredients)) {
      deals[id] = eligible.filter(p => matches(id,p)).sort((a,b) => b.discount-a.discount || a.price-b.price || a.name.localeCompare(b.name))[0] || null;
    }
    return {deals,eligibleCount:eligible.length};
  }
  function generate(products, supplied = {}) {
    const options = {servings:2,retailer:'Whole Foods',store:'',prime:true,vegetarian:false,pantry:[],...supplied};
    options.servings = Math.max(1,Math.min(12,Math.floor(Number(options.servings) || 2)));
    const {deals,eligibleCount} = analyze(products,options);
    const usage = {}, totals = {}, days = [];
    for (let day=0;day<7;day++) {
      const meals = ['breakfast','lunch','dinner'].map(slot => {
        const candidates = recipes.filter(r => r.slot === slot && (!options.vegetarian || r.vegetarian));
        const score = r => {
          const ids = Object.keys(r.items).filter(id => !['oil','salt','seasoning'].includes(id));
          return ids.reduce((sum,id) => sum+(deals[id] ? 1+deals[id].discount/100 : 0),0)/ids.length - (usage[r.id] || 0)*2;
        };
        candidates.sort((a,b) => score(b)-score(a) || a.id-b.id);
        const recipe = candidates[0];
        usage[recipe.id] = (usage[recipe.id] || 0)+1;
        const items = Object.entries(recipe.items).map(([id,quantity]) => {
          const amount = quantity*options.servings;
          if (!totals[id]) totals[id] = {id,name:ingredients[id][0],unit:ingredients[id][1],group:ingredients[id][3],quantity:0,deal:deals[id],meals:[]};
          totals[id].quantity += amount;
          totals[id].meals.push(`Day ${day+1} ${slot}`);
          return {id,name:ingredients[id][0],quantity:amount,unit:ingredients[id][1]};
        });
        return {...recipe,items,saleIngredients:items.filter(i => deals[i.id]).map(i => i.id)};
      });
      days.push({day:day+1,meals});
    }
    const groceries = Object.values(totals).map(item => ({...item,quantity:Math.round(item.quantity*100)/100,pantry:options.pantry.includes(item.id)}));
    groceries.sort((a,b) => a.group.localeCompare(b.group) || a.name.localeCompare(b.name));
    return {options,days,groceries,eligibleCount,matchedCount:groceries.filter(i=>i.deal).length};
  }
  const api = {generate,analyze,matches,money,ingredients,recipes};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.MealPlanner = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
