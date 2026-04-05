import hashlib
import json
import re
from typing import Any, Optional, Tuple

from playwright.sync_api import sync_playwright


TARGET_GROCERY_DEALS_URL = "https://www.target.com/c/grocery-deals/-/N-k4uyq"
LOAD_WAIT_MS = 1800
MAX_LOAD_MORE_CLICKS = 30


def normalize_text_key(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def build_offer_id(name: str, value_text: Optional[str], expires: Optional[str]) -> str:
    digest = hashlib.sha1(f"{name}|{value_text or ''}|{expires or ''}".encode("utf-8")).hexdigest()[:16]
    return f"target:{digest}"


def parse_offer_value(value_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not value_text:
        return None, None

    cleaned = re.sub(r"\s+", " ", value_text).strip()
    price_match = re.search(r"\$\d+(?:\.\d{1,2})?(?:\s*-\s*\$\d+(?:\.\d{1,2})?)?", cleaned)
    current_price = None
    discount = None

    if price_match:
        current_price = price_match.group(0).replace(" - ", " to ")

    if "%" in cleaned or cleaned.lower().startswith("buy") or "bogo" in cleaned.lower():
        discount = cleaned

    return current_price, discount


def parse_offer_card(card) -> Optional[dict[str, Any]]:
    name = None
    value_text = None
    expires = None
    image = None

    show_items = card.locator('button[aria-label^="Show items for "]')
    if show_items.count():
        aria_label = show_items.first.get_attribute("aria-label") or ""
        name = re.sub(r"^Show items for\s+", "", aria_label).strip()

    title = card.locator('[data-test="offer-title"]')
    if title.count():
        title_text = title.first.inner_text().strip()
        if not name:
            name = title_text
    if not name:
        return None

    value = card.locator('div[class*="OfferCardValue"]')
    if value.count():
        value_text = value.first.inner_text().strip()

    subtitle = card.locator('p')
    if subtitle.count():
        expires = subtitle.first.inner_text().strip()

    img = card.locator("img")
    if img.count():
        image = img.first.get_attribute("src")

    current_price, discount = parse_offer_value(value_text)
    if not current_price and not discount:
        return None

    return {
        "asin": build_offer_id(name, value_text, expires),
        "name": name,
        "brand": None,
        "variation": None,
        "image": image,
        "url": None,
        "current_price": current_price,
        "basis_price": None,
        "prime_price": None,
        "discount": discount,
        "unit_price": None,
        "retailer": "Target",
        "expires": expires,
        "target_value_text": value_text,
    }


def discover_target_deals() -> dict[str, Any]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 1600})
        print(f"Opening Target grocery deals page: {TARGET_GROCERY_DEALS_URL}")
        page.goto(TARGET_GROCERY_DEALS_URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(5000)
        page.locator("text=/\\d+ results/i").first.wait_for(timeout=30000)
        page.locator('[data-test="offer-card"]').first.wait_for(timeout=30000)

        load_more_clicks = 0
        stale_rounds = 0
        previous_count = 0

        while load_more_clicks < MAX_LOAD_MORE_CLICKS:
            cards = page.locator('[data-test="offer-card"]')
            current_count = cards.count()
            load_more = page.locator("button", has_text="Load more")
            print(
                f"Target round {load_more_clicks + 1}: visible offer cards {current_count}, "
                f"load_more={'y' if load_more.count() else 'n'}"
            )

            if not load_more.count():
                break

            load_more.first.scroll_into_view_if_needed()
            load_more.first.click()
            page.wait_for_timeout(LOAD_WAIT_MS)
            load_more_clicks += 1

            new_count = cards.count()
            if new_count <= current_count and new_count <= previous_count:
                stale_rounds += 1
            else:
                stale_rounds = 0
            previous_count = new_count

            if stale_rounds >= 2:
                print("Target: stopping because Load more no longer reveals new offer cards.")
                break

        result_count_text = page.locator("text=/\\d+ results/i").first.inner_text().strip()
        cards = page.locator('[data-test="offer-card"]')
        parsed: dict[str, dict[str, Any]] = {}
        for index in range(cards.count()):
            product = parse_offer_card(cards.nth(index))
            if not product:
                continue
            parsed.setdefault(product["asin"], product)

        browser.close()

    products = sorted(parsed.values(), key=lambda item: normalize_text_key(item.get("name")))
    return {
        "source_url": TARGET_GROCERY_DEALS_URL,
        "result_count_text": result_count_text,
        "product_count": len(products),
        "load_more_clicks": load_more_clicks,
        "products": products,
    }


if __name__ == "__main__":
    result = discover_target_deals()
    print(json.dumps(
        {
            "source_url": result["source_url"],
            "result_count_text": result["result_count_text"],
            "product_count": result["product_count"],
            "load_more_clicks": result["load_more_clicks"],
        },
        indent=2,
    ))
