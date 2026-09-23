import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import app
from offer_quality import clean_prices, offer_issues

NOW = datetime.now(timezone.utc)
def offer(**changes):
    value = dict(store_id='10160',current_price='$2.77/lb',prime_price='$2.49',basis_price='$3.29/lb',
                 observed_at=NOW.isoformat(),price_verified=True,price_context='Pickup',price_source='Search Deals')
    value.update(changes)
    return value

class OfferQualityTests(unittest.TestCase):
    def test_prime_and_nonprime_are_distinct_and_units_preserved(self):
        p=app.standardize_product_record(name='Zucchini',regular_price='$3.29/lb',current_price='$2.77/lb',prime_price='$2.49')
        self.assertEqual(p['prime_price'],'$2.49/lb')
        self.assertEqual(p['current_price'],'$2.77/lb')
        p=app.standardize_product_record(name='Corn',current_price='$4.99')
        self.assertIsNone(p['prime_price'])
        self.assertIsNone(p['discount'])

    def test_no_fabricated_price_from_percent_or_range(self):
        p=clean_prices(dict(current_price='11% off',prime_price='20% off',basis_price='$5.19 to $6.29/lb'))
        self.assertIsNone(p['current_price'])
        self.assertIsNone(p['prime_price'])
        self.assertIsNone(p['basis_price'])

    def test_new_observation_clears_old_prime_discount(self):
        old=offer(observed_at=(NOW-timedelta(hours=1)).isoformat())
        new=offer(current_price='$4.99',prime_price=None,basis_price=None,discount=None,discount_percent=0)
        merged=app.merge_store_offers([old],new)[0]
        self.assertIsNone(merged['prime_price'])
        p=dict(prime_price='$1.99',store_offers=[merged])
        app.apply_primary_store_offer(p)
        self.assertIsNone(p['prime_price'])

    def test_undated_stale_expired_and_unavailable_quarantined(self):
        for changes in [dict(observed_at=None),dict(observed_at=(NOW-timedelta(days=4)).isoformat()),dict(expires='Expires May 30'),dict(expires=(NOW-timedelta(days=1)).isoformat()),dict(availability='OUT_OF_STOCK')]:
            self.assertTrue(offer_issues(offer(**changes),NOW))
        self.assertFalse(offer_issues(offer(),NOW))

    def test_grouped_flyer_never_inherits_product_prices(self):
        promo=dict(promotionId='abc',productName='Cut fruit',salePrice='11% off',primePrice='20% off',regularPrice='$6.29/lb',asinsList=['a','b'])
        p=app.build_flyer_display_product(promo)
        self.assertEqual(p['asin'],'flyer:abc')
        self.assertEqual(p['offer_kind'],'promotion')
        self.assertIn('/promotion/abc',p['url'])
        detail=app.standardize_flyer_detail_product(promo,dict(asin='a',name='Cantaloupe'),1,2)
        self.assertIsNone(detail['current_price'])
        self.assertIsNone(detail['prime_price'])

    def test_newsletter_honors_nonprime_and_does_not_send_prime_only_deals(self):
        p=dict(key='zucchini',name='Zucchini',retailer='Whole Foods',**offer())
        p=clean_prices(p)
        result=app.newsletter_candidates([p],{'newsletterPreferences':{'prime':False}})
        self.assertEqual(result[0]['newsletter_price'],'$2.77/lb')
        self.assertEqual(result[0]['discount_percent'],16)
        self.assertEqual(result[0]['newsletter_price_label'],'Sale price')
        p['current_price']='$3.29/lb'
        self.assertEqual(app.newsletter_candidates([p],{'newsletterPreferences':{'prime':False}}),[])

    def test_public_catalog_retains_only_usable_store_offers(self):
        p=dict(name='Zucchini',retailer='Whole Foods',store_offers=[offer(),offer(store_id='other',observed_at=None)])
        with patch('app.load_base_combined_products',return_value=[p]):
            public=app.load_combined_products()
        self.assertEqual(public[0]['available_store_ids'],['10160'])
        self.assertEqual(len(public[0]['store_offers']),1)

class SourceCategoryTests(unittest.TestCase):
    def test_source_category_beats_flavor_brand_and_cached_prediction(self):
        from catalog_rules import apply_source_rules
        cases=[('Ginger nude gloss','/Beauty/Makeup/','Beauty & Personal Care'),('Gogono Kocha','/Beverage & Coffee & Tea & Honey/Tea/','Beverages'),('Whole DHA milk','/Dairy & Egg/Milk & Cheese & Yogurt & Butter/','Dairy & Eggs')]
        for name,source,expected in cases:
            p=apply_source_rules(dict(name=name,retailer='H Mart',category='Produce',source_categories=[source]))
            self.assertEqual(p['category'],expected)

class ImportRegressionTests(unittest.TestCase):
    def test_product_name_is_not_truncated_to_brand(self):
        name='365 by Whole Foods Market, Organic Yellow Creamer Potato Bag, 24 Ounce'
        self.assertIn('Potato', app.clean_display_name(name))

    def test_selling_unit_is_not_comparison_unit(self):
        from discover_search_deals import normalize_products_api_item
        item=dict(asin='test',variableUnitOfMeasure={'pricingUom':{'dimension':'WEIGHT','unit':'POUNDS'}},offerDetails={'price':{'priceAmount':5.42,'basisPriceAmount':6.09,'primeBenefit':{'priceAmount':4.88}}})
        self.assertEqual(normalize_products_api_item(item)['prime_price'],'$4.88/lb')
        item['variableUnitOfMeasure']['pricingUom']={'dimension':'COUNT','unit':'UNITS'}
        item['offerDetails']['unitPrice']={'baseUnit':'ounce','priceAmount':0.2}
        self.assertEqual(normalize_products_api_item(item)['current_price'],'$5.42')

    def test_catering_cookie_does_not_verify_pickup_context(self):
        from discover_search_deals import wait_for_selected_store
        from unittest.mock import Mock
        page=Mock()
        with patch('discover_search_deals.get_selected_store_text',return_value='Delivery to 10001'), patch('discover_search_deals.get_page_store_context',return_value={'store_id':'10160'}):
            self.assertFalse(wait_for_selected_store(page,{'id':'10160','name':'Columbus Circle'},timeout_ms=300))

class TaxonomyRegressionTests(unittest.TestCase):
    def test_token_boundaries_do_not_find_ham_in_chamomile(self):
        from taxonomy_ai import text_has_any
        self.assertFalse(text_has_any('chamomile tea', ['ham']))
        self.assertFalse(text_has_any('veggie burger', ['egg']))
        self.assertTrue(text_has_any('smoked ham', ['ham']))

    def test_product_form_precedes_flavor_and_claims(self):
        from taxonomy_ai import source_backed_classification
        from fixed_taxonomy import FIXED_TAXONOMY
        cases=[('Roman Chamomile in Jojoba Oil','Beauty & Personal Care'),('Primal Kitchen Creamy Cashew Alfredo','Pantry'),('Organic Chamomile with Lavender Herbal Tea','Beverages'),('Menstrual Cups','Beauty & Personal Care'),('Flax Pecan Crunch Cereal','Pantry'),('Black Bean Veggie Burger','Meat & Seafood'),('Root Beer Soda','Beverages')]
        for name,category in cases:
            self.assertEqual(source_backed_classification({'name':name}, FIXED_TAXONOMY)['category'],category,name)

    def test_low_confidence_guesses_are_failed_and_withheld(self):
        from taxonomy_ai import apply_failed_classification_bucket
        p=apply_failed_classification_bucket(dict(category='Dairy & Eggs',subcategory='Cheese',ai_confidence=0.035,**offer()))
        self.assertEqual(p['classification_status'],'failed')
        with patch('app.load_base_combined_products',return_value=[p]):
            self.assertEqual(app.load_combined_products(),[])

class ScrapeMergeTests(unittest.TestCase):
    def test_new_scrape_snapshot_clears_discontinued_prime_offer(self):
        from discover_search_deals import merge_product
        merged=merge_product({'current_price':'$3','prime_price':'$2','basis_price':'$4'}, {'current_price':'$4','prime_price':None,'basis_price':None})
        self.assertIsNone(merged['prime_price'])
        self.assertIsNone(merged['basis_price'])

    def test_refresh_does_not_override_failed_unit_verification(self):
        from offer_quality import stamp_products
        p=stamp_products([dict(price_verified=False)],'Search Deals','Pickup')[0]
        self.assertFalse(p['price_verified'])
