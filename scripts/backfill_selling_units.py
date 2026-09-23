"""Repair lost selling units from PDP metadata, without importing another store's prices."""
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from offer_quality import offer_issues


def pricing_uom(asin):
    try:
        response = requests.get(f'https://www.wholefoodsmarket.com/grocery/product/{asin}', timeout=20)
        response.raise_for_status()
        text = response.text.replace('\\"', '"')
        match = re.search(r'"pricingUom"\s*:\s*(\{[^{}]+\})', text)
        return json.loads(match[1]) if match else None
    except (requests.RequestException, ValueError):
        return None


def main():
    path = Path(__file__).resolve().parents[1] / 'search_deals_products.json'
    products = json.loads(path.read_text())
    catalog = json.loads((path.parent / 'combined_products.json').read_text())
    weighted_departments = {p.get('asin') for p in catalog if p.get('category') in ('Produce', 'Meat & Seafood', 'Dairy & Eggs')}
    candidates = [p for p in products if not offer_issues(p) and not p.get('pricing_uom') and '/' not in (p.get('current_price') or '') and (not p.get('unit_price') or p.get('asin') in weighted_departments)]
    units = dict(zip([p['asin'] for p in candidates], ThreadPoolExecutor(max_workers=4).map(pricing_uom, [p['asin'] for p in candidates])))
    repaired = withheld = 0
    for p in candidates:
        uom = units.get(p['asin'])
        suffix = {'POUNDS':'/lb','OUNCES':'/oz','KILOGRAMS':'/kg','GRAMS':'/g'}.get((uom or {}).get('unit'))
        if not uom or (uom.get('dimension') == 'WEIGHT' and not suffix):
            p['price_verified'] = False
            p['unit_evidence'] = 'Selling unit could not be verified; withheld pending refresh'
            withheld += 1
            continue
        p['pricing_uom'] = uom
        p['unit_evidence'] = 'Retailer product metadata; prices and their original store observation retained'
        if suffix:
            for key in ('current_price','prime_price','basis_price'):
                if p.get(key) and '/' not in p[key]: p[key] += suffix
            repaired += 1
    path.write_text(json.dumps(products,indent=2,ensure_ascii=False)+'\n')
    print(f'Checked {len(candidates)} missing units; repaired {repaired} weighted offers; withheld {withheld} unresolved offers')

if __name__ == '__main__':
    main()
