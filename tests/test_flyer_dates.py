import unittest
from datetime import datetime, timezone
from offer_quality import flyer_expiry, offer_issues


class FlyerDateTests(unittest.TestCase):
    def test_inclusive_end_date_covers_final_evening_only(self):
        expiry = flyer_expiry('2026-10-06T05:00:00Z')
        self.assertEqual(expiry, '2026-10-07T04:00:00+00:00')
        offer = dict(observed_at='2026-10-06T20:00:00Z', expires=expiry,
                     offer_kind='promotion', price_verified=True,
                     price_context='Weekly flyer', promotion_terms={'sale':'20% off'})
        self.assertNotIn('Expired offer', offer_issues(offer,datetime(2026,10,6,23,tzinfo=timezone.utc)))
        self.assertIn('Expired offer', offer_issues(offer,datetime(2026,10,7,4,1,tzinfo=timezone.utc)))
