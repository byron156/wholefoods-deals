import unittest
from unittest.mock import patch
import app


class NewsletterHistoryTests(unittest.TestCase):
    @patch('app.load_recent_newsletter_deliveries_from_supabase', return_value=[])
    @patch('app.load_newsletter_state_local')
    def test_missing_device_ids_do_not_match_unrelated_subscribers(self, local, remote):
        local.return_value = {'deliveries': [
            {'subscriber_id': 'other', 'status': 'sent', 'payload_metadata': {'product_keys': ['unrelated']}},
            {'subscriber_id': 'mine', 'status': 'sent', 'payload_metadata': {'product_keys': ['mine']}},
            {'subscriber_id': 'mine', 'status': 'failed', 'payload_metadata': {'product_keys': ['failed']}},
        ]}
        self.assertEqual(app.recent_newsletter_product_keys({'id': 'mine'}), ['mine'])
        self.assertEqual(app.recent_newsletter_product_keys(None), [])

    def test_meal_planner_route_is_available(self):
        with app.app.test_client() as client:
            response = client.get('/meal-plan/')
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'id="planner-data"', response.data)
            self.assertIn(b'Weekly Meal Plan', response.data)


if __name__ == '__main__':
    unittest.main()
