"""Reconcile retailer promotion identities with the actual shopper catalog."""
import html
from offer_quality import offer_issues


def reconcile(report, products):
    visible = {}
    for product in products:
        if product.get('offer_kind') != 'promotion':
            continue
        # Synthetic IDs preserve distinct promotions, including same-name offers.
        parts = (product.get('asin') or '').split(':')
        if len(parts) < 2 or parts[0] != 'flyer':
            continue
        for offer in product.get('store_offers') or [product]:
            if offer_issues(offer):
                continue
            sid = str(offer.get('store_id') or offer.get('source_store_id') or '')
            visible.setdefault((sid, parts[1]), set()).update(offer.get('eligible_asins') or [])
            if offer.get('eligible_asin'):
                visible[(sid, parts[1])].add(offer['eligible_asin'])
    missing = []
    for promo in report['promotions']:
        key = (str(promo['store_id']), promo['promotion_id'])
        absent = sorted(set(promo['eligible_asins']) - visible.get(key, set()))
        if key not in visible or absent:
            missing.append({'store_id':key[0], 'promotion_id':key[1], 'missing_asins':absent})
    return dict(report, missing_from_catalog=missing,
                displayed_store_promotions=len(report['promotions'])-len(missing))


def render_coverage(report):
    if not report:
        return ''
    return f'''<section class="callout"><h2>Advertised sale coverage</h2>
<p>{html.escape(report['scope'])}. Collected {html.escape(report['generated_at'])}.</p>
<p><strong>{report.get('displayed_store_promotions', 0)} / {report['advertised_store_promotions']} advertised store promotions represented</strong>,
covering {report['eligible_store_products']:,} product memberships across promotions and stores.
These memberships are not a count of unique products.</p>
<p>{len(report.get('missing_from_catalog', []))} promotions missing from the catalog.
{report.get('metadata_gaps', 0)} eligible memberships lack product details from the retailer;
they remain attached to their published promotion, with the full eligibility list retained.
This measures weekly flyer coverage, not every online-only sale or other retailer.</p>
<a href="sale_coverage.json">Download the source-by-source reconciliation</a></section>'''
