import unittest
from unittest.mock import patch
from datetime import timedelta
from itsdangerous import URLSafeTimedSerializer
import app


def product(index, **kwargs):
    return dict(key=f'p{index}', name=f'Item {index}', retailer='Whole Foods', category='Pantry',
                brand='Example', observed_at=app.iso_utc(app.utcnow()),price_verified=True,price_context='Pickup', current_price='$4.00', prime_price='$3.60', basis_price='$5.00', **kwargs)


class DigestTests(unittest.TestCase):
    def digest(self, products, preferences=None):
        profile = {'newsletterPreferences': preferences or {}}
        with patch('app.profile_for_subscriber', return_value=profile), patch('app.create_feedback_token', return_value='test-token'):
            return app.generate_newsletter_digest_for_subscriber({'id': 'test'}, products=products)

    def test_digest_exact_size_unique_and_empty_catalog(self):
        products = [product(i) for i in range(40)]
        for length in [4, 8, 12, 24]:
            digest = self.digest(products, {'digestLength': length, 'preferredCategories': ['Pantry','Produce','Dairy & Eggs']})
            keys = [row['product_key'] for row in digest['snapshot']]
            self.assertEqual(len(keys), length)
            self.assertEqual(len(set(keys)), length)
        self.assertEqual(self.digest([])['snapshot'], [])

    def test_filters_and_selected_store_prime_prices(self):
        items = [product(i) for i in range(6)]
        items[0]['brand'] = 'Hidden'
        items[1]['category'] = 'Snacks'
        items[2]['expires'] = '2020-01-01'
        items[3]['prime_price'] = '$8.00'
        items[4]['store_offers'] = [{'observed_at':app.iso_utc(app.utcnow()),'price_verified':True,'price_context':'Pickup','store_id':'a','prime_price':'$2.00','basis_price':'$5.00'}]
        profile = {'newsletterPreferences': {'hiddenBrands':['hidden'],'dislikedCategories':['Snacks'],'preferredStoreIds':['a']}}
        result = app.newsletter_candidates(items,profile)
        self.assertEqual([p['key'] for p in result], ['p4'])
        self.assertEqual(result[0]['newsletter_price'],'$2.00')
        self.assertEqual(result[0]['discount_percent'],60)

    def test_email_escapes_content_and_links_meal_plan(self):
        item = product(0)
        item['name'] = '<script>unsafe</script>'
        item['url'] = 'javascript:alert(1)'
        html = app.render_digest_html(self.digest([item]))
        self.assertNotIn('<script>',html)
        self.assertNotIn('javascript:',html)
        self.assertIn('&lt;script&gt;',html)
        self.assertIn('/meal-plan/',html)
        self.assertIn('Prime price',html)
        self.assertIn('Unsubscribe',html)

    @patch('app.load_latest_newsletter_delivery_from_supabase',return_value=None)
    @patch('app.load_newsletter_state_local')
    def test_skips_do_not_restart_cadence(self,local,remote):
        sent = {'subscriber_id':'test','status':'sent','sent_at':app.iso_utc(app.utcnow()-timedelta(days=8)),'digest_date':'2000-01-01'}
        local.return_value={'deliveries':[sent,dict(sent,status='skipped',sent_at=app.iso_utc(app.utcnow()))]}
        latest=app.latest_newsletter_delivery({'id':'test'})
        self.assertEqual(latest['status'],'sent')
        self.assertTrue(app.cadence_allows_delivery({'cadence':'weekly'},latest,'2026-09-20'))
        sent['sent_at']=app.iso_utc(app.utcnow()-timedelta(days=6))
        self.assertFalse(app.cadence_allows_delivery({'cadence':'weekly'},sent,'2026-09-20'))

    @patch('app.send_newsletter_email')
    @patch('app.save_newsletter_delivery')
    @patch('app.profile_for_subscriber', return_value={})
    @patch('app.recent_newsletter_product_keys', return_value=[])
    @patch('app.latest_newsletter_delivery', return_value=None)
    @patch('app.list_active_newsletter_subscribers', return_value=[{'id':'test','email':'test@example.com'}])
    def test_empty_catalog_does_not_send(self, subscribers, latest, recent, profile, save, send):
        app.send_newsletter_digests(products=[])
        send.assert_not_called()
        self.assertEqual(save.call_args.args[3]['reason'], 'no_matching_deals')

    @patch('app.supabase_enabled', return_value=True)
    @patch('app.list_active_newsletter_subscribers_from_supabase', return_value=[])
    @patch('app.load_newsletter_state_local')
    def test_empty_remote_subscriptions_do_not_revive_stale_local_records(self, local, remote, enabled):
        self.assertEqual(app.list_active_newsletter_subscribers(), [])
        local.assert_not_called()

    @patch('app.save_device_profile')
    @patch('app.load_device_profile',return_value={})
    @patch('app.save_local_newsletter_subscriber')
    @patch('app.upsert_newsletter_subscriber_to_supabase',return_value={'status':'unsubscribed'})
    @patch('app.load_newsletter_subscriber_by_id',return_value={'id':'test','device_id':'dev','email':'test@example.com','status':'active'})
    def test_unsubscribe_requires_signed_link_and_confirmation(self,load,remote,local,profile,save):
        token=URLSafeTimedSerializer(app.app.config['SECRET_KEY'],salt='newsletter-unsubscribe').dumps({'id':'test'})
        with app.app.test_client() as client:
            self.assertEqual(client.get('/api/newsletter/unsubscribe/bad').status_code,400)
            self.assertEqual(client.get(f'/api/newsletter/unsubscribe/{token}').status_code,200)
            remote.assert_not_called()
            response=client.post(f'/api/newsletter/unsubscribe/{token}')
            self.assertIn('unsubscribed',response.get_data(as_text=True))
            self.assertEqual(remote.call_args.kwargs['status'],'unsubscribed')
            self.assertFalse(save.call_args.args[1]['newsletterEnabled'])

if __name__ == '__main__':
    unittest.main()
