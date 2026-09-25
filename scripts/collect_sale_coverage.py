"""Collect every advertised Whole Foods promotion and its full eligibility list."""
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import requests
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import app
from discover_search_deals import normalize_products_api_item
from offer_quality import metadata
from scripts.recover_catalog import STORE_CONTEXTS, fetch_batch


def get_json(url, params=None):
    last = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            last = exc
    raise RuntimeError(f'Retailer request failed: {url}') from last


def collect_eligibility(store_id, promotion, fetch=get_json):
    """Count source rows separately from unique IDs: retailer lists contain duplicates."""
    rows, expected, page = [], None, 1
    seen_pages = set()
    while expected is None or len(rows) < expected:
        data = fetch('https://www.wholefoodsmarket.com/api/wwos/sales-flyer/grouped-promotion',
                     {'storeId':store_id,'promotionId':promotion['promotionId'],'pageNum':page,'pageSize':100})
        batch = data.get('asins')
        total = data.get('totalAvailableAsins')
        if not isinstance(batch,list) or not isinstance(total,int) or total < 0:
            raise ValueError('Invalid promotion eligibility response')
        if expected is not None and expected != total:
            raise ValueError('Promotion changed during pagination; retry collection')
        expected = total
        if not batch and len(rows) < expected:
            raise ValueError('Eligibility pagination ended before retailer total')
        signature = tuple(batch)
        if signature in seen_pages and batch:
            raise ValueError("Retailer repeated an eligibility page")
        seen_pages.add(signature)
        rows.extend(batch)
        page += 1
        if page > 100:
            raise ValueError('Eligibility pagination exceeded safety bound')
    if len(rows) != expected or any(not isinstance(a, str) or len(a) != 10 for a in rows):
        raise ValueError("Eligibility rows do not match the retailer total or identity format")
    return sorted(set(rows)), len(rows), expected


def collect_store(sid):
    response = requests.get(app.build_sales_flyer_url(sid),timeout=30)
    response.raise_for_status()
    props = app.extract_next_data_from_html(response.text)['props']['pageProps']
    if str(props.get('storeId')) != sid:
        raise ValueError('Retailer flyer returned wrong store')
    promotions = props['promotions']
    if not isinstance(promotions, list) or not promotions:
        raise ValueError('Retailer returned no promotion listing; preserve previous collection')
    if len({p.get('promotionId') for p in promotions}) != len(promotions):
        raise ValueError('Duplicate promotion identities in retailer listing')
    eligibility = []
    for promo in promotions:
        if promo.get('promotionGrouping') == 'GROUPED':
            ids, rows, expected = collect_eligibility(sid,promo)
        else:
            ids = sorted(set(promo.get('asinsList') or ([promo['asin']] if promo.get('asin') else [])))
            rows = expected = len(ids)
        eligibility.append((promo,ids,rows,expected))
    ids = sorted({asin for _, members, _, _ in eligibility for asin in members})
    product_map = {}
    for offset in range(0,len(ids),20):
        raw = get_json('https://www.wholefoodsmarket.com/api/wwos/products',{'store':sid,'asins':','.join(ids[offset:offset+20])})
        # This endpoint returns product metadata even when online pricing is absent.
        if raw and isinstance(raw[0],list): raw = raw[0]
        product_map.update({p['asin']:p for p in raw if isinstance(p,dict) and p.get('asin')})
    missing = sorted(set(ids)-set(product_map))
    if missing:
        _, _, recovered, error = fetch_batch(sid, missing)
        product_map.update({p['asin']:p for p in recovered if p.get('name')})
    # Historical identity metadata is useful; historical prices are never reused.
    known = json.loads((ROOT/'combined_products.json').read_text())
    for p in known:
        asin = p.get('asin')
        if asin in ids and asin not in product_map and p.get('name'):
            product_map[asin] = {'asin':asin,'name':p['name'],'brandName':p.get('brand'),
                                 '_retained_identity':True}
    missing = sorted(set(ids)-set(product_map))
    products, coverage = [], []
    for promo, members, rows, expected in eligibility:
        parent = app.build_flyer_display_product(dict(promo,store_id=sid,store_name=STORE_CONTEXTS[sid][0]))
        parent['eligible_asins'] = members
        # Keep every membership on its advertised promotion even if the retailer
        # supplies no product identity details. Do not fabricate a product name.
        parent['unresolved_eligible_asins'] = sorted(set(members)-set(product_map))
        products.append(parent)
        for asin in members:
            if asin not in product_map:
                continue
            raw = product_map[asin]
            detail = normalize_products_api_item(raw)
            child = dict(parent, asin=f"flyer:{promo['promotionId']}:{asin}",
                         asins=[f"flyer:{promo['promotionId']}:{asin}"],
                         name=raw['name'],raw_name=raw['name'],brand=raw.get('brandName'),
                         source_brand=raw.get('brandName'),brand_source='source',
                         image=detail.get('image') or parent.get('image'),eligible_asin=asin,
                         brand_is_generic=False,flyer_source='verified-eligibility',
                         current_price=None,prime_price=None,basis_price=None,discount=None,discount_percent=0,
                         **metadata(detail))
            child.pop('eligible_asins', None)
            child.pop('unresolved_eligible_asins', None)
            child['metadata_observed_at'] = None if raw.get('_retained_identity') else parent['observed_at']
            # Keep the promotion URL/terms as the authority. Never transfer an
            # in-store percentage into the separate online dollar-price channel.
            products.append(child)
        coverage.append({'store_id':sid,'promotion_id':promo['promotionId'],'name':promo['productName'],
                         'url':parent['url'],'source_rows':rows,'expected_rows':expected,
                         'duplicate_source_rows':rows-len(members),'eligible_asins':members,
                         'resolved_products':sum(a in product_map for a in members),
                         'unresolved_metadata_asins':parent['unresolved_eligible_asins'],'captured':True})
    print(f'{sid}: {len(promotions)} promotions, {len(ids)} unique eligible products',flush=True)
    return products,coverage


def main():
    # All stores must finish before replacing the last successful collection.
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(collect_store, STORE_CONTEXTS))
    products=[p for items,_ in results for p in items]
    coverage=[p for _,items in results for p in items]
    report={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'Whole Foods weekly promotions at the two supported stores',
            'advertised_store_promotions':len(coverage),'captured_store_promotions':len(coverage),
            'eligible_store_products':sum(len(p['eligible_asins']) for p in coverage),
            'unique_eligible_products':len({a for p in coverage for a in p['eligible_asins']}),
            'resolved_product_memberships':sum(p['resolved_products'] for p in coverage),
            'metadata_gaps':sum(len(p['unresolved_metadata_asins']) for p in coverage),
            'missing_store_promotions':0,'promotions':coverage}
    for path,payload in [('flyer_products.json',products),('reports/sale_coverage.json',report), ('flyer_report.json', {
            'generated_at':report['generated_at'], 'product_count':len(products),
            'stores':[{'store_id':sid,'store_name':STORE_CONTEXTS[sid][0],
                       'product_count':len(items),'reused_previous':False}
                      for sid,(items,_) in zip(STORE_CONTEXTS,results)],
            'coverage_report':'reports/sale_coverage.json'})]:
        dest=ROOT/path;tmp=dest.with_suffix('.partial.json');tmp.write_text(json.dumps(payload,indent=2,ensure_ascii=False)+'\n');tmp.replace(dest)
    print('Complete:',report['advertised_store_promotions'],'store promotions;',report['eligible_store_products'],'eligible product memberships')

if __name__=='__main__': main()
