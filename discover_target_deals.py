import hashlib
import json
import re
from typing import Any, Optional, Tuple
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright


TARGET_GROCERY_DEALS_URL = "https://www.target.com/c/grocery-deals/-/N-k4uyq"
LOAD_WAIT_MS = 1800
MAX_LOAD_MORE_CLICKS = 30
DETAIL_WAIT_MS = 400
DETAIL_DIALOG_TIMEOUT_MS = 2500
MULTISTORY_LINK_SELECTOR = '[data-test="@web/slingshot-components/MultiStory/Link"]'


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


def normalize_target_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href.replace("http://", "https://", 1)
    if href.startswith("/"):
        return "https://www.target.com" + href
    return None


def build_target_search_url(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return f"https://www.target.com/s?searchTerm={quote_plus(name)}"


def dismiss_target_popups(page) -> None:
    for locator in [
        page.locator('button[aria-label="close"]').first,
        page.locator('button[aria-label="Close"]').first,
        page.locator("button", has_text=re.compile(r"not now|maybe later|close", re.I)).first,
    ]:
        try:
            if locator.count():
                locator.click(timeout=1000, force=True)
                page.wait_for_timeout(250)
        except Exception:
            pass


def extract_offer_url(page, card) -> Optional[str]:
    direct_link = card.locator('a[href*="/p/"], a[href*="/pl/"]').first
    if direct_link.count():
        href = direct_link.get_attribute("href")
        normalized = normalize_target_url(href)
        if normalized:
            return normalized

    show_items = card.locator('button[aria-label^="Show items for "]')
    if not show_items.count():
        return None

    try:
        show_items.first.click(force=True, timeout=2000)
        page.wait_for_timeout(DETAIL_WAIT_MS)
        dialog = page.locator('[role="dialog"]').last
        dialog.wait_for(timeout=DETAIL_DIALOG_TIMEOUT_MS)

        href = None
        show_all = dialog.locator('[data-test="eligible-items-carousel-show-all-link"]').first
        if show_all.count():
            href = show_all.get_attribute("href")

        if not href:
            product_link = dialog.locator('a[data-test="@web/OfferDetails/EligibleItemsCard/Link"]').first
            if product_link.count():
                href = product_link.get_attribute("href")

        close_button = dialog.locator('button[aria-label="close"]').first
        if close_button.count():
            close_button.click(timeout=1000)
            page.wait_for_timeout(150)

        return normalize_target_url(href)
    except Exception:
        try:
            dialog = page.locator('[role="dialog"]').last
            close_button = dialog.locator('button[aria-label="close"]').first
            if close_button.count():
                close_button.click(timeout=1000)
                page.wait_for_timeout(150)
        except Exception:
            pass
        return None


def parse_offer_card(card, page) -> Optional[dict[str, Any]]:
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

    url = extract_offer_url(page, card) or build_target_search_url(name)

    return {
        "asin": build_offer_id(name, value_text, expires),
        "name": name,
        "brand": None,
        "variation": None,
        "image": image,
        "url": url,
        "current_price": current_price,
        "basis_price": None,
        "prime_price": None,
        "discount": discount,
        "unit_price": None,
        "retailer": "Target",
        "expires": expires,
        "target_value_text": value_text,
    }


def parse_multistory_deal_link(link) -> Optional[dict[str, Any]]:
    text = re.sub(r"\s+", " ", link.inner_text().replace("", " ")).strip()
    if not text:
        return None

    href = normalize_target_url(link.get_attribute("href"))
    parts = [part.strip(" *") for part in re.split(r"\s{2,}|\s+\|\s+", text) if part.strip(" *")]
    if not parts:
        parts = [text]

    value_text = None
    name = None
    for part in parts:
        lowered = part.lower()
        if re.search(r"\d+\s*%|bogo|\$\d+|\d+/\$\d+|save when|buy \d+", lowered):
            value_text = value_text or part
        elif part:
            name = part

    name = name or parts[-1]
    if not name:
        return None

    current_price, discount = parse_offer_value(value_text or text)
    discount = discount or value_text or text

    return {
        "asin": build_offer_id(name, value_text or text, None),
        "name": name,
        "brand": None,
        "variation": None,
        "image": None,
        "url": href or build_target_search_url(name),
        "current_price": current_price,
        "basis_price": None,
        "prime_price": None,
        "discount": discount,
        "unit_price": None,
        "retailer": "Target",
        "expires": None,
        "target_value_text": value_text or text,
    }


def discover_target_deals() -> dict[str, Any]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 1600})
        print(f"Opening Target grocery deals page: {TARGET_GROCERY_DEALS_URL}")
        page.goto(TARGET_GROCERY_DEALS_URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(5000)
        dismiss_target_popups(page)
        page.locator("text=/\\d+ results/i").first.wait_for(timeout=30000)

        offer_cards = page.locator('[data-test="offer-card"]')
        multistory_links = page.locator(MULTISTORY_LINK_SELECTOR)
        try:
            offer_cards.first.wait_for(timeout=12000)
        except Exception:
            dismiss_target_popups(page)
            if not multistory_links.count():
                page.screenshot(path="logs/target_deals_no_offers.png", full_page=True)
                raise RuntimeError("Target did not render old offer cards or new grocery promo links.")
            print(
                "Target old offer-card layout did not render; parsing visible grocery promo links instead."
            )

        load_more_clicks = 0
        stale_rounds = 0
        previous_count = 0

        while offer_cards.count() and load_more_clicks < MAX_LOAD_MORE_CLICKS:
            current_count = offer_cards.count()
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
            dismiss_target_popups(page)
            load_more_clicks += 1

            new_count = offer_cards.count()
            if new_count <= current_count and new_count <= previous_count:
                stale_rounds += 1
            else:
                stale_rounds = 0
            previous_count = new_count

            if stale_rounds >= 2:
                print("Target: stopping because Load more no longer reveals new offer cards.")
                break

        result_count_text = page.locator("text=/\\d+ results/i").first.inner_text().strip()
        parsed: dict[str, dict[str, Any]] = {}
        if offer_cards.count():
            for index in range(offer_cards.count()):
                if index and index % 25 == 0:
                    print(f"Target parse: processed {index}/{offer_cards.count()} offers")
                product = parse_offer_card(offer_cards.nth(index), page)
                if not product:
                    continue
                parsed.setdefault(product["asin"], product)
        else:
            for index in range(multistory_links.count()):
                product = parse_multistory_deal_link(multistory_links.nth(index))
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
