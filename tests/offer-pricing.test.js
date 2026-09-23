const test=require('node:test');const assert=require('node:assert/strict');
const pricing=require('../static/offer-pricing');const planner=require('../static/meal-planner');
const evidence={observed_at:new Date().toISOString(),price_verified:true,price_context:'Pickup'};
const p={...evidence,name:'Organic broccoli',retailer:'Whole Foods',category:'Produce',current_price:'$2.77/lb',prime_price:'$2.49/lb',basis_price:'$3.29/lb'};
test('Prime toggle changes actual price and recalculates savings',()=>{assert.equal(pricing.select(p,true).display_price,'$2.49/lb');assert.equal(pricing.select(p,false).discount_percent,16);});
test('selection uses selected store and never carries another store Prime price',()=>{const r=pricing.select({...p,store_offers:[{...evidence,store_id:'b',current_price:'$3.00',basis_price:'$4.00'}]},true,['b']);assert.equal(r.display_price,'$3.00');assert.equal(r.is_prime,false);assert.equal(pricing.select({...p,store_offers:[{...evidence,store_id:'b'}]},true,['a']),null);});
test('percentage, range, and stale offers cannot become ingredient deals',()=>{assert.equal(pricing.money('$5.19 to $6.29/lb'),null);assert.equal(pricing.money('20% off'),null);for(const item of [{...p,offer_kind:'promotion'},{...p,price_verified:true,price_context:'Pickup',observed_at:'2020-01-01'}])assert.equal(planner.generate([item]).matchedCount,0);});

test('unverified legacy rows are withheld',()=>{assert.equal(pricing.select({current_price:'$2',basis_price:'$3'}),null);});
