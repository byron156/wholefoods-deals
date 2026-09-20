const test = require('node:test');
const assert = require('node:assert/strict');
const planner = require('../static/meal-planner');
const deal = (name, extra={}) => ({name,category:'Produce',retailer:'Whole Foods',current_price:'$3.00',basis_price:'$6.00',...extra});

test('complete week, diverse dinners, and exact consolidated quantities', () => {
  const plan = planner.generate([],{servings:3});
  assert.equal(plan.days.length,7);
  assert.equal(plan.days.flatMap(d=>d.meals).length,21);
  assert.equal(new Set(plan.days.map(d=>d.meals[2].id)).size,7);
  const quantities = {};
  for (const day of plan.days) for (const meal of day.meals) for (const item of meal.items) quantities[item.id]=(quantities[item.id]||0)+item.quantity;
  for (const item of plan.groceries) assert.equal(item.quantity,quantities[item.id]);
  const single = planner.generate([],{servings:1});
  for (const item of plan.groceries) assert.equal(item.quantity,3*single.groceries.find(i=>i.id===item.id).quantity);
});
test('sales influence the menu and vegetarian plans exclude meat', () => {
  const products = [deal('Atlantic salmon fillet',{category:'Meat & Seafood'}),deal('Broccoli')];
  const plan = planner.generate(products);
  assert.equal(plan.days[0].meals[2].name,'Salmon broccoli rice bowls');
  const vegetarian = planner.generate(products,{vegetarian:true});
  assert.ok(vegetarian.days.every(d=>d.meals.every(m=>m.vegetarian)));
  assert.ok(!vegetarian.groceries.some(i=>['chicken','salmon'].includes(i.id)));
});
test('location, retailer, Prime, expired offers, and missing prices are respected', () => {
  const p = deal('Broccoli',{store_offers:[{store_id:'a',current_price:'$5.00',basis_price:'$6.00',prime_price:'$4.00'}]});
  assert.equal(planner.generate([p],{store:'a',prime:false}).groceries.find(i=>i.id==='broccoli').deal.price,5);
  assert.equal(planner.generate([p],{store:'a',prime:true}).groceries.find(i=>i.id==='broccoli').deal.price,4);
  for (const opts of [{store:'b'},{retailer:'Target'}]) assert.equal(planner.generate([p],opts).matchedCount,0);
  assert.equal(planner.generate([deal('Broccoli',{expires:'2020-01-01'})]).matchedCount,0);
  assert.equal(planner.generate([deal('Broccoli',{current_price:null})]).matchedCount,0);
  assert.equal(planner.generate([deal('Broccoli',{basis_price:'$2'})]).matchedCount,0);
});
test('ingredient matches reject prepared foods and misleading names even with bad taxonomy', () => {
  const wrong = [
    ['eggs','Egg Nest Pappardelle'], ['rice','Brown Rice Protein Powder'],
    ['spinach','Ravioli Spinach and Cheese Organic'],['banana','Strawberry Banana Yogurt'],
    ['milk','Whole Milk Yogurt'],['oil','Olive Oil Popcorn'],['salt','Sea Salt Popcorn'],
    ['carrot','Kombucha Carrot Organic'],['cheese','Hot Cheddar Cheezmos'],
    ['chicken',"Kevin's Thai Coconut Chicken Breast"],['salmon','Smoked Salmon Fillet'],
    ['beans','Dry Black Beans'], ['rice','Brown Rice Pasta'],
    ['milk','Evanhealy Blue Lavender Cleansing Milk'], ['milk','Black Milk Tea 11.8 fl.oz'],
    ['cheese',"Annie's Shells & White Cheddar"], ['cheese','White Cheddar Harvest Snaps, 3 Ounce'],
    ['salt','Organic Quinoa & Brown Rice with Sea Salt'],
    ['banana','Vosges Haut-Chocolat Super Dark Coconut Ash and Banana'],
    ['oil','Ithaca Hummus Olive Oil Sea Salt, 10OZ'],
  ];
  for (const [id,name] of wrong) assert.equal(planner.matches(id,deal(name)),false,name);
  assert.equal(planner.matches('broccoli',deal('Broccoli Florets')),true);
  assert.equal(planner.matches('beans',deal('Organic Black Beans, 15 Ounce',{category:'Pantry'})),true);
});
test('invalid serving counts are bounded and pantry quantities remain available', () => {
  assert.equal(planner.generate([],{servings:-2}).options.servings,1);
  assert.equal(planner.generate([],{servings:100}).options.servings,12);
  const p=planner.generate([],{pantry:['oil']});
  assert.ok(p.groceries.find(i=>i.id==='oil').pantry);
});

test('Prime prices are the default with a non-Prime opt-out', () => {
  const item=deal('Broccoli',{prime_price:'$2.00'});
  const find=options=>planner.generate([item],options).groceries.find(i=>i.id==='broccoli').deal;
  assert.equal(find({}).price,2);
  assert.equal(find({}).isPrime,true);
  assert.equal(find({prime:false}).price,3);
  assert.equal(find({prime:false}).isPrime,false);
});

test('missing location Prime price never falls back to another store', () => {
  const item=deal('Broccoli',{prime_price:'$1.00',store_offers:[{store_id:'a',current_price:'$4.00',basis_price:'$5.00'}]});
  const matched=planner.generate([item],{store:'a'}).groceries.find(i=>i.id==='broccoli').deal;
  assert.equal(matched.price,4);
  assert.equal(matched.isPrime,false);
});
