import unittest
import json
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from scripts.collect_sale_coverage import collect_eligibility
from scripts.sale_coverage import reconcile
from app import combined_key_for_product


class SaleCoverageTests(unittest.TestCase):
    def test_online_pagination_includes_valid_boundary(self):
        from discover_search_deals import network_offsets
        offsets = network_offsets(500)
        self.assertEqual(offsets[-2:], [480, 500])
        self.assertEqual(len(offsets), len(set(offsets)))

    def test_online_overlap_does_not_hide_later_new_products(self):
        import discover_search_deals as crawler
        products = {f'B{i:09}': {'asin':f'B{i:09}'} for i in range(60)}
        def batch(page, discriminator, label, rank, offset, **kwargs):
            ids = [f'B{i:09}' for i in range(offset, min(offset+30,70))]
            return {'ok':True}, ids, 70, 30, 200
        def detail(page, url):
            from urllib.parse import urlparse, parse_qs
            ids = parse_qs(urlparse(url).query)['asins'][0].split(',')
            return 200, json.dumps([{'asin':a} for a in ids])
        with patch.multiple(crawler,
                            dismiss_popups=Mock(), wait_for_search_results=Mock(),
                            merge_products_from_current_page=Mock(return_value=0),
                            extract_offer_listing_discriminator=Mock(return_value='A08F'),
                            fetch_rsi_payload_with_tail_fallback=Mock(side_effect=batch),
                            fetch_text_via_page=Mock(side_effect=detail),
                            normalize_products_api_item=lambda p:p,
                            merge_product=lambda a,b:b):
            result = crawler.crawl_current_sort_via_network(Mock(), products, 'Test', 'relevanceblender')
        self.assertEqual(len(products),70)
        self.assertTrue(result['listing_complete'])

    def test_pagination_preserves_all_unique_members_and_counts_duplicates(self):
        pages = [[f'B{i:09}' for i in range(100)], ['B000000099','B000000100']]
        def fetch(url, params):
            return {'asins':pages[params['pageNum']-1], 'totalAvailableAsins':102}
        ids, rows, expected = collect_eligibility('10160', {'promotionId':'p'}, fetch)
        self.assertEqual((len(ids), rows, expected), (101,102,102))

    def test_truncated_or_overfull_response_is_failure(self):
        for rows, total in [([],1),(['B000000001'],0)]:
            with self.assertRaises(ValueError):
                collect_eligibility('10160', {'promotionId':'p'}, lambda *args: {'asins':rows,'totalAvailableAsins':total})

    def test_same_name_promotions_do_not_merge(self):
        a = dict(offer_kind='promotion',name='Coffee',asin='flyer:first')
        b = dict(a,asin='flyer:second')
        self.assertNotEqual(combined_key_for_product(a),combined_key_for_product(b))

    def test_reconciliation_checks_store_membership_and_expiry(self):
        report = {'promotions':[{'store_id':'10160','promotion_id':'p','eligible_asins':['B000000001']} ]}
        offer = dict(store_id='10160',offer_kind='promotion',price_verified=True,
                     price_context='Weekly flyer',observed_at=datetime.now(timezone.utc).isoformat(),
                     promotion_terms={'sale':'20% off'}, eligible_asins=['B000000001'])
        product = dict(asin='flyer:p',offer_kind='promotion',store_offers=[offer])
        self.assertEqual(reconcile(report,[product])['missing_from_catalog'],[])
        offer['store_id']='10328'
        self.assertEqual(len(reconcile(report,[product])['missing_from_catalog']),1)
        offer.update(store_id='10160',expires='2020-01-01')
        self.assertEqual(len(reconcile(report,[product])['missing_from_catalog']),1)

if __name__ == '__main__': unittest.main()
