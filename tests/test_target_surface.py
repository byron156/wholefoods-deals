import unittest
from unittest.mock import patch
from discover_target_deals import MULTISTORY_LINK_SELECTOR, wait_for_target_deals_surface


class TargetSurfaceTest(unittest.TestCase):
    def test_new_promotion_layout_does_not_require_results_label(self):
        class Locator:
            def __init__(self, count):
                self.value = count
            def count(self):
                return self.value
        class Page:
            def locator(self, selector):
                if selector == MULTISTORY_LINK_SELECTOR:
                    return Locator(12)
                if selector == '[data-test="offer-card"]':
                    return Locator(0)
                # Popup dismissals use other selectors, which can safely fail.
                raise AssertionError('No results count exists on the new layout')
        with patch("discover_target_deals.dismiss_target_popups"):
            wait_for_target_deals_surface(Page(), timeout_ms=1000)
