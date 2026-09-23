/* Price selection is shared by browsing and meal planning; no promotion arithmetic. */
(function(root) {
  'use strict';
  const money = text => {const m = String(text || '').match(/^\s*\$(\d+(?:\.\d{1,2})?)(?:\s*(?:\/\s*(?:lb|oz|kg|g|each)|ea|each))?\s*$/i);return m ? Number(m[1]) : null;};
  function usable(offer, now = Date.now()) {
    const observed = Date.parse(offer.observed_at);
    return offer.price_verified === true && Boolean(offer.price_context) && Number.isFinite(observed) && now-observed <= 72*3600000 && observed-now <= 300000 && (!offer.expires || (Number.isFinite(Date.parse(offer.expires)) && Date.parse(offer.expires)>now)) && (!offer.starts_at || Date.parse(offer.starts_at)<=now) && !['OUT_OF_STOCK','UNAVAILABLE','NO_CURRENT_OFFER'].includes(offer.availability);
  }
  function select(product, prime = true, storeIds = []) {
    let offer = product;
    if (product.retailer === 'Whole Foods' && (product.store_offers || []).length) {
      const offers = product.store_offers.filter(o => (!storeIds.length || storeIds.includes(String(o.store_id))) && usable(o));
      offers.sort((a,b) => (money((prime && a.prime_price) || a.current_price) ?? Infinity)-(money((prime && b.prime_price) || b.current_price) ?? Infinity));
      if (!offers.length) return null;
      offer = {...product, ...Object.fromEntries(['current_price','prime_price','basis_price','discount','discount_percent','sale_price'].map(k=>[k,null])), ...offers[0]};
    }
    if (!usable(offer)) return null;
    const priceText = (prime && offer.prime_price) || offer.current_price || '';
    const price = money(priceText), regular = money(offer.basis_price || (prime && offer.prime_price ? offer.current_price : null));
    const discount = price && regular && price<regular ? Math.round((1-price/regular)*100) : 0;
    return {...offer, display_price:priceText, is_prime:Boolean(prime && offer.prime_price && product.retailer==='Whole Foods'), discount_percent:discount, discount:discount ? `${discount}% off` : null};
  }
  function readPrime(fallback = true) {try {const value = localStorage.getItem('grocery-prime-prices');return value===null ? fallback : value !== 'false';}catch (_) {return fallback;}}
  function savePrime(value) {try {localStorage.setItem('grocery-prime-prices',String(Boolean(value)));}catch (_) {}}
  const api = {money,usable,select,readPrime,savePrime};
  if (typeof module !== 'undefined' && module.exports) module.exports=api;
  else root.OfferPricing=api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
