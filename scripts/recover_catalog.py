"""Refresh known Whole Foods products using store-scoped retailer API evidence.

The discriminators below come from retailer pageProps.wfmccLocationData for the
matching store IDs, not from a default location. Never import default PDP prices.
"""
import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import requests
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from discover_search_deals import build_products_url, normalize_products_api_item

STORE_CONTEXTS = {'10160': ('Columbus Circle', 'A08F'), '10328': ('Upper West Side', 'A08G')}


def fetch_batch(store_id, asins):
    name, discriminator = STORE_CONTEXTS[store_id]
    url = build_products_url(discriminator, asins)
    error = None
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list):
                raise ValueError('Expected retailer product array')
            return store_id, asins, rows, None
        except (requests.RequestException, ValueError) as exc:
            error = type(exc).__name__
            if attempt < 2: time.sleep(attempt + 1)
    return store_id, asins, [], error


def normalize_observation(raw, sid, checked):
    """Keep metadata and prices attached to their store-scoped observation."""
    p=normalize_products_api_item(raw)
    p.update(available_store_ids=[sid],source_store_id=sid,source_store_name=STORE_CONTEXTS[sid][0],
             observed_at=checked,price_context='Store-specific online',price_source='Whole Foods product API',
             retailer='Whole Foods',retailer_product_type=(raw.get('category') or {}).get('productType'),
             retailer_department=(raw.get('category') or {}).get('glProductGroupSymbol'),
             retailer_description=raw.get('description'),retailer_ingredients=raw.get('ingredients'),
             metadata_observed_at=checked,offer_listing_discriminator=STORE_CONTEXTS[sid][1])
    if not raw.get('offerDetails'):
        p['availability']='NO_CURRENT_OFFER'
        p['price_verified']=True # Retailer explicitly returned no offer; not a parser failure.
    return p


def main():
    catalog = json.loads((ROOT / 'combined_products.json').read_text())
    previous = json.loads((ROOT / 'search_deals_products.json').read_text())
    prior_report = json.loads((ROOT / 'search_deals_report.json').read_text())
    listing_runs = prior_report.get('listing_runs') or [s for s in prior_report.get('stores', []) if s.get('sort_runs')]
    asins = sorted({p.get('asin') for p in catalog if p.get('retailer') == 'Whole Foods' and re.fullmatch(r'[A-Z0-9]{10}', p.get('asin') or '')})
    # Include prior identities even if classification/merging excluded them.
    asins = sorted(set(asins) | {p['asin'] for p in previous if re.fullmatch(r'[A-Z0-9]{10}',p.get('asin') or '')})
    flyer = json.loads((ROOT / 'flyer_products.json').read_text())
    asins = sorted(set(asins) | {p['eligible_asin'] for p in flyer if p.get('eligible_asin')}
                   | {a for p in flyer for a in (p.get('eligible_asins') or [])})
    existing = {(str((p.get('available_store_ids') or [''])[0]), p.get('asin')):p for p in previous}
    products, metadata, failures = [], {}, []
    checked = datetime.now(timezone.utc).isoformat()
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = [pool.submit(fetch_batch, sid, asins[i:i+20]) for sid in STORE_CONTEXTS for i in range(0,len(asins),20)]
        for index, job in enumerate(as_completed(jobs),1):
            sid, requested, rows, error = job.result()
            received = set()
            for raw in rows:
                if not isinstance(raw,dict) or raw.get('asin') not in requested: continue
                asin=raw['asin']; received.add(asin)
                p=normalize_observation(raw, sid, checked)
                products.append(p)
                metadata[asin]={k:p.get(k) for k in ('retailer_product_type','retailer_department','retailer_description','retailer_ingredients','metadata_observed_at')}
            for asin in set(requested)-received:
                failures.append({'store_id':sid,'asin':asin,'reason':error or 'Product missing from retailer response'})
                if (sid,asin) in existing: products.append(existing[(sid,asin)])
            if index % 20 == 0: print(f'{index}/{len(jobs)} batches; {len(products)} store observations',flush=True)
    products.sort(key=lambda p:(p.get('source_store_id') or '',p['asin']))
    (ROOT/'search_deals_products.json').write_text(json.dumps(products,indent=2,ensure_ascii=False)+'\n')
    report={'generated_at':checked,'listing_runs':listing_runs,'requested_identities':len(asins),'observations':len(products),'fresh_observations':sum(p.get('observed_at') == checked for p in products),'retained_previous_observations':sum(p.get('observed_at') != checked for p in products),'failures':failures,
            'product_types':dict(Counter(p.get('retailer_product_type') or 'MISSING' for p in products)),
            'stores':[{'store_id':sid,'store_name':name,'product_count':sum(p.get('source_store_id')==sid for p in products),
                       'reused_previous':any(f['store_id']==sid for f in failures),'source':'Store-scoped product API'} for sid,(name,_) in STORE_CONTEXTS.items()]}
    (ROOT/'reports/catalog_recovery.json').write_text(json.dumps(report,indent=2)+'\n')
    (ROOT/'search_deals_report.json').write_text(json.dumps(report,indent=2)+'\n')
    (ROOT/'retailer_product_metadata.json').write_text(json.dumps(metadata,indent=2,ensure_ascii=False)+'\n')
    print('Recovery complete:',len(products),'observations;',len(failures),'unresolved responses',flush=True)

if __name__=='__main__': main()
