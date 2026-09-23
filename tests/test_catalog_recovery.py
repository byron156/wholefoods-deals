import unittest
from datetime import datetime, timezone
from catalog_rules import apply_source_rules
from retailer_categories import TYPE_PAIRS
from fixed_taxonomy import FIXED_TAXONOMY
from offer_quality import offer_data_issues, offer_issues
from scripts.catalog_quality_audit import data_quality_reasons
from discover_target_deals import normalize_promotion_text
from test_offer_quality import offer


class RetailerEvidenceTests(unittest.TestCase):
    def test_all_source_pairs_exist_in_taxonomy(self):
        pairs = {(c['name'],s['name']) for c in FIXED_TAXONOMY['categories'] for s in c['subcategories']}
        self.assertFalse(set(TYPE_PAIRS.values()) - pairs)

    def test_retailer_type_recovers_failed_labels_without_trusting_bad_types(self):
        cases = [
            ('Organic Zucchini Squash','VEGETABLE','Produce','Vegetables'),
            ('Organic Frozen Broccoli','VEGETABLE','Frozen','Frozen Vegetables'),
            ('Cold Smoked Salmon','PET_FOOD','Meat & Seafood','Smoked Seafood'),
            ('Olive Oil','ANIMAL_NUTRITIONAL_SUPPLEMENT','Pantry','Oils & Vinegars'),
            ('Shampoo with Avocado & Olive Oil','SHAMPOO','Beauty & Personal Care','Hair Care'),
            ('Whole Milk Kefir','SCULPTURE','Dairy & Eggs','Yogurt'),
            ('Chocolate Syrup','DEHUMIDIFIER','Pantry','Sweeteners'),
            ('Tea Black Unsweet','CULINARY_SALT','Beverages','Ready-to-Drink Tea'),
            ('Protein Whey Chocolate Peanut Butter','PROTEIN_SUPPLEMENT_POWDER','Supplements & Wellness','Protein & Collagen'),
            ('Whey Protein Chocolate Peanut Butter','PROTEIN_SUPPLEMENT_POWDER','Supplements & Wellness','Protein & Collagen'),
            ('Frozen Yogurt Bars','SNACK_FOOD_BAR','Frozen','Frozen Desserts'),
            ('Plant Based Meatballs','FOOD','Meat & Seafood','Meat Alternatives'),
            ('Bar Chocolate Original Oat Milk Mini','CHOCOLATE_CANDY','Snacks','Candy & Chocolate'),
            ('Aldens Organic Strawberry Yogurt Bars','SNACK_FOOD_BAR','Frozen','Frozen Desserts'),
            ('Lime Hard Seltzer','BEER','Alcohol','Hard Seltzer'),
        ]
        for name,kind,category,subcategory in cases:
            with self.subTest(name=name):
                p=apply_source_rules(dict(name=name,retailer_product_type=kind,metadata_observed_at=datetime.now(timezone.utc).isoformat(),classification_status='failed'))
                self.assertEqual((p.get('category'),p.get('subcategory')),(category,subcategory))
                self.assertEqual(p['classification_status'],'classified')

    def test_generic_source_type_does_not_manufacture_category(self):
        p=apply_source_rules(dict(name='Mystery Blend',retailer_product_type='FOOD',metadata_observed_at=datetime.now(timezone.utc).isoformat(),classification_status='failed'))
        self.assertEqual(p['classification_status'],'failed')

    def test_unavailable_is_known_state_not_missing_data(self):
        p=offer(current_price=None,prime_price=None,availability='NO_CURRENT_OFFER')
        self.assertIn('Unavailable',offer_issues(p))
        self.assertEqual(offer_data_issues(p),[])
        p['observed_at']=None
        self.assertIn('Missing observation time',offer_data_issues(p))

    def test_secondary_store_data_gaps_remain_reported(self):
        p=dict(image='image',url='url',store_offers=[offer(),offer(observed_at=None)])
        self.assertIn('Missing observation time',data_quality_reasons(p))

    def test_target_promotions_do_not_turn_bogo_into_product_discount(self):
        self.assertEqual(normalize_promotion_text('BOGO 50 BOGO 50% off candy corn'),('candy corn','BOGO 50% off'))
        self.assertEqual(normalize_promotion_text('10 10% off trail mix'),('trail mix','10% off'))
        self.assertIsNone(normalize_promotion_text('Buy $50 get a $5 gift card'))

if __name__=='__main__': unittest.main()
