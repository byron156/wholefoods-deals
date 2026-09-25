"""Offer validation: keep price, location, channel and observation time together."""
import re
from datetime import datetime, timedelta, timezone

PRICE_FIELDS = ('current_price', 'prime_price', 'basis_price', 'sale_price', 'discount', 'discount_percent', 'unit_price')
EVIDENCE_FIELDS = ('eligible_asins', 'unresolved_eligible_asins', 'eligible_asin', 'observed_at', 'expires', 'starts_at', 'price_context', 'price_source', 'price_verified', 'offer_kind', 'source_store_name', 'availability', 'pricing_uom', 'unit_evidence')
METADATA_FIELDS = ('retailer_product_type', 'retailer_department', 'retailer_description', 'retailer_ingredients', 'metadata_observed_at')

def metadata(product):
    return {key: product.get(key) for key in METADATA_FIELDS}

MAX_AGE = timedelta(hours=72)


def money(value):
    match = re.fullmatch(r'\s*\$(\d+(?:\.\d{1,2})?)(?:\s*(?:/\s*(?:lb|oz|kg|g|each)|ea|each))?\s*', str(value or ''), re.I)
    return float(match[1]) if match else None


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except (ValueError, TypeError):
        return None


def evidence(product):
    return {key: product.get(key) for key in EVIDENCE_FIELDS}


def stamp_products(products, source, context):
    now = datetime.now(timezone.utc).isoformat()
    for product in products:
        if product.get("retailer_product_type"):
            product["metadata_observed_at"] = now
        product.update(observed_at=now, price_source=source, price_context=context, price_verified=product.get("price_verified") is not False)
    return products


def offer_issues(offer, now=None):
    now = now or datetime.now(timezone.utc)
    issues = []
    observed = timestamp(offer.get('observed_at'))
    if not observed:
        issues.append('Missing observation time')
    elif observed > now + timedelta(minutes=5) or now - observed > MAX_AGE:
        issues.append('Stale observation')
    if offer.get('price_verified') is not True:
        issues.append('Unverified price context')
    if not offer.get('price_context'):
        issues.append('Missing price context')
    if offer.get('expires'):
        expiry = timestamp(offer['expires'])
        if not expiry:
            issues.append('Unparseable expiry')
        elif expiry <= now:
            issues.append('Expired offer')
    starts = timestamp(offer.get('starts_at'))
    if starts and starts > now:
        issues.append('Offer has not started')
    if offer.get('availability') in ('OUT_OF_STOCK', 'UNAVAILABLE', 'NO_CURRENT_OFFER'):
        issues.append('Unavailable')
    if offer.get('offer_kind') != 'promotion' and not any(money(offer.get(k)) for k in ('current_price', 'prime_price')):
        issues.append('Missing exact selling price')
    return issues


def clean_prices(product):
    """No percentage/range becomes a guessed product price; Prime stays distinct."""
    out = dict(product)
    for key in ('current_price', 'prime_price', 'basis_price', 'sale_price'):
        if money(out.get(key)) is None:
            out[key] = None
    # A Prime benefit often omits the per-weight suffix carried by its sale price.
    reference = out.get('current_price') or out.get('basis_price') or ''
    suffix = re.search(r'(/\s*(?:lb|oz|kg|g)|\s+ea)$', reference, re.I)
    if suffix and out.get('prime_price') and not re.search(r'/|\bea\b', out['prime_price']):
        out['prime_price'] += suffix[0]
    price = money(out.get('prime_price') or out.get('current_price'))
    regular = money(out.get('basis_price') or (out.get('current_price') if out.get('prime_price') else None))
    pct = round((1 - price / regular) * 100) if price and regular and price < regular else 0
    out['discount_percent'] = pct
    out['discount'] = f'{pct}% off' if pct else None
    if out.get('offer_kind') == 'promotion':
        out['promotion_terms'] = product.get('promotion_terms') or {}
    return out


def offer_data_issues(offer, now=None):
    """Acquisition/validation failures, distinct from a known offer lifecycle."""
    issues = offer_issues(offer, now)
    known_absence = offer.get('availability') in ('OUT_OF_STOCK', 'UNAVAILABLE', 'NO_CURRENT_OFFER')
    lifecycle = {'Unavailable', 'Expired offer', 'Offer has not started'}
    if 'Expired offer' in issues and offer.get('price_verified') is True and timestamp(offer.get('observed_at')):
        lifecycle.add('Stale observation')
    if known_absence and offer.get('price_verified') is True:
        lifecycle.add('Missing exact selling price')
    return [issue for issue in issues if issue not in lifecycle]
