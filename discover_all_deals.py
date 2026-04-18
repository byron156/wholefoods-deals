import json
import re
import sys
import time
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


STORE_MODAL_URL = "https://www.wholefoodsmarket.com/stores?modalView=true"
ALL_DEALS_URL = "https://www.wholefoodsmarket.com/fmc/alldeals/?_encoding=UTF8&almBrandId=VUZHIFdob2xlIEZvb2Rz&ref_=US_TRF_ALL_UFG_WFM_REFER_0428801"

STORE_SEARCH_TEXT = "Columbus Circle"
STORE_SELECT_CTA_PATTERN = re.compile(
    r"make this my store|shop store|select store|choose store|set as my store",
    re.I,
)

MAX_SCROLL_ROUNDS = 140
SCROLL_PAUSE_MS = 350
FINAL_SETTLE_MS = 1200

PROGRESS_BAR_WIDTH = 56
SET_STORE_PROGRESS = 0.14
OPEN_DEALS_PROGRESS = 0.20
SCROLL_PROGRESS_START = 0.20
SCROLL_PROGRESS_END = 0.95
FINAL_PARSE_PROGRESS = 0.98

INITIAL_TOTAL_ETA_SECONDS = 55


def emoji_for_product(name: Optional[str]) -> str:
    if not name:
        return "🛒"

    name = name.lower()

    if "avocado" in name:
        return "🥑"
    if "milk" in name or "creamer" in name:
        return "🥛"
    if "chicken" in name:
        return "🍗"
    if "beef" in name or "lamb" in name or "ham" in name:
        return "🥩"
    if "salmon" in name or "fish" in name:
        return "🐟"
    if "shrimp" in name:
        return "🍤"
    if "strawberry" in name or "strawberries" in name:
        return "🍓"
    if "apple" in name:
        return "🍎"
    if "bread" in name:
        return "🍞"
    if "croissant" in name:
        return "🥐"
    if "cheese" in name:
        return "🧀"
    if "lemon" in name:
        return "🍋"
    if "orange" in name:
        return "🍊"
    if "mango" in name:
        return "🥭"
    if "kiwi" in name:
        return "🥝"
    if "broccoli" in name:
        return "🥦"
    if "egg" in name:
        return "🥚"
    if "dates" in name:
        return "🌴"
    if "quiche" in name:
        return "🥧"
    if "bacon" in name:
        return "🥓"
    if "potato" in name and "sweet" not in name:
        return "🥔"
    if "potato" in name and "sweet" in name:
        return "🍠"
    if "pancake" in name:
        return "🥞"
    if "juice" in name:
        return "🧃"
    if "rice" in name:
        return "🍚"
    if "cake" in name:
        return "🍰"
    if "chocolate" in name:
        return "🍫"
    if "blueberry" in name or "blueberries" in name:
        return "🫐"
    if "popcorn" in name:
        return "🍿"
    if "corn" in name:
        return "🌽"

    return "🛒"


def parse_prime_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None

    text = text.strip()
    match = re.search(r"Join Prime to buy this item at\s+(\$[\d.,]+(?:/\w+)?)", text)
    if match:
        return match.group(1)

    return text


def parse_card_recommendation(rec_str: str) -> dict:
    rec = json.loads(rec_str)
    return {
        "id": rec["id"],
        "index": rec["index"],
        "raw": rec_str,
    }


def parse_all_deals_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    products = []

    for cell in soup.select('[id^="gridCell-"]'):
        cell_id = cell.get("id", "")
        asin = cell_id.replace("gridCell-", "", 1) if cell_id.startswith("gridCell-") else None

        name = None
        name_el = cell.select_one(".a-truncate-full")
        if name_el:
            name = name_el.get_text(" ", strip=True)

        image = None
        img_el = cell.select_one("img")
        if img_el:
            image = img_el.get("src")

        current_price = None
        current_offscreen = cell.select_one("span.a-price > span.a-offscreen")
        if current_offscreen:
            current_price = current_offscreen.get_text(strip=True)

        regular_price = None
        strike_el = cell.select_one("._c3ViY_strikeThroughPrice_1P5lG")
        if strike_el:
            regular_price = strike_el.get_text(strip=True)

        discount = None
        promo_badge = cell.select_one("div[id^='price-discount-promotion-'] span[id='promotion-text']")
        if promo_badge:
            discount = promo_badge.get_text(" ", strip=True)

        prime_price = None
        prime_el = cell.select_one("div[id^='prime-upsell-promotion-'] span[id='promotion-text']")
        if prime_el:
            prime_price = parse_prime_text(prime_el.get_text(" ", strip=True))

        unit_price = None
        unit_el = cell.select_one("._c3ViY_pricePerUnit_1J0Bq")
        if unit_el:
            unit_price = unit_el.get_text(" ", strip=True)

        url = None
        link_el = cell.select_one("a[id^='alm-cards-desktop-link-location-']")
        if link_el:
            href = link_el.get("href")
            if href:
                url = urljoin("https://www.wholefoodsmarket.com", href)

        products.append({
            "asin": asin,
            "name": name,
            "image": image,
            "url": url,
            "current_price": current_price,
            "basis_price": regular_price,
            "prime_price": prime_price,
            "discount": discount,
            "unit_price": unit_price,
            "emoji": emoji_for_product(name),
        })

    return products


class ProgressBar:
    def __init__(self, width: int = 56, initial_eta_seconds: int = 55):
        self.width = width
        self.start_time = time.time()
        self.displayed_progress = 0.0
        self.initial_eta_seconds = max(1, int(initial_eta_seconds))
        self.total_expected_seconds = float(self.initial_eta_seconds)

    def _format_eta(self, seconds_remaining: float) -> str:
        seconds_remaining = max(0, int(round(seconds_remaining)))
        mins, secs = divmod(seconds_remaining, 60)
        if mins > 99:
            mins = 99
            secs = 59
        return f"{mins:02d}:{secs:02d}"

    def update(self, target_progress: float) -> None:
        now = time.time()
        elapsed = max(0.0, now - self.start_time)

        target_progress = max(0.0, min(1.0, target_progress))

        min_step = 0.0025
        max_step = 0.03
        desired_step = target_progress - self.displayed_progress

        if desired_step > 0:
            self.displayed_progress += min(max(desired_step, min_step), max_step)
            self.displayed_progress = min(self.displayed_progress, target_progress)

        eta = max(0.0, self.total_expected_seconds - elapsed)
        eta_str = self._format_eta(eta)

        filled = int(self.width * self.displayed_progress)
        if filled <= 0:
            bar = ">" + " " * (self.width - 1)
        elif filled >= self.width:
            bar = "=" * self.width
        else:
            bar = "=" * (filled - 1) + ">" + " " * (self.width - filled)

        sys.stdout.write(f"\r[{bar}] ETA {eta_str}")
        sys.stdout.flush()

    def animate_wait(self, start_progress: float, end_progress: float, total_ms: int, steps: int = 8) -> None:
        if total_ms <= 0:
            self.update(end_progress)
            return

        sleep_s = total_ms / 1000 / steps
        for i in range(1, steps + 1):
            frac = i / steps
            prog = start_progress + (end_progress - start_progress) * frac
            self.update(prog)
            time.sleep(sleep_s)

    def finish(self) -> None:
        self.displayed_progress = 1.0
        sys.stdout.write(f"\r[{'=' * self.width}] ETA 00:00\n")
        sys.stdout.flush()


def dismiss_popups(page) -> None:
    popup_texts = [
        "Accept",
        "Accept All",
        "Allow all",
        "Got it",
        "Close",
        "Dismiss",
        "No thanks",
        "Not now",
    ]

    for text in popup_texts:
        try:
            locator = page.get_by_role("button", name=re.compile(f"^{re.escape(text)}$", re.I))
            if locator.count() > 0:
                locator.first.click(timeout=800)
                page.wait_for_timeout(150)
        except Exception:
            pass


def click_first(locators, timeout=2500) -> bool:
    for locator in locators:
        try:
            count = locator.count()
            for i in range(min(count, 10)):
                candidate = locator.nth(i)

                try:
                    candidate.scroll_into_view_if_needed(timeout=800)
                except Exception:
                    pass

                try:
                    candidate.click(timeout=timeout)
                    return True
                except Exception:
                    try:
                        candidate.click(timeout=timeout, force=True)
                        return True
                    except Exception:
                        continue
        except Exception:
            continue
    return False


def click_first_no_wait(locators, timeout=2500) -> bool:
    for locator in locators:
        try:
            count = locator.count()
            for i in range(min(count, 12)):
                candidate = locator.nth(i)
                try:
                    candidate.scroll_into_view_if_needed(timeout=800)
                except Exception:
                    pass

                try:
                    candidate.click(timeout=timeout, no_wait_after=True)
                    return True
                except Exception:
                    try:
                        candidate.click(timeout=timeout, force=True, no_wait_after=True)
                        return True
                    except Exception:
                        try:
                            candidate.evaluate(
                                """
                                (el) => {
                                    el.scrollIntoView({ block: "center", inline: "center" });
                                    ["mousedown", "mouseup", "click"].forEach((type) => {
                                        el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
                                    });
                                    if (typeof el.click === "function") el.click();
                                    return true;
                                }
                                """
                            )
                            return True
                        except Exception:
                            continue
        except Exception:
            continue
    return False


def debug_body(page) -> None:
    try:
        body = page.locator("body").inner_text(timeout=4000)
        print("\nBODY SAMPLE:")
        print(body[:2500])
    except Exception:
        pass


def dump_inputs(scope, label="scope") -> None:
    try:
        inputs = scope.locator("input")
        count = inputs.count()
        print(f"\nINPUTS IN {label}: {count}")
        for i in range(min(count, 20)):
            try:
                el = inputs.nth(i)
                print(
                    "INPUT",
                    i,
                    {
                        "type": el.get_attribute("type"),
                        "name": el.get_attribute("name"),
                        "id": el.get_attribute("id"),
                        "placeholder": el.get_attribute("placeholder"),
                        "aria-label": el.get_attribute("aria-label"),
                    },
                )
            except Exception:
                pass
    except Exception:
        pass


def get_store_modal(page):
    candidates = [
        page.get_by_role("dialog"),
        page.locator('[role="dialog"]'),
        page.locator('[aria-modal="true"]'),
        page.locator("div.ReactModal__Content"),
        page.locator("div[class*='modal']"),
        page.locator("div[class*='Modal']"),
        page.locator("div[class*='sheet']"),
        page.locator("div[class*='Sheet']"),
        page.locator("div[class*='drawer']"),
        page.locator("div[class*='Drawer']"),
    ]

    for locator in candidates:
        try:
            count = locator.count()
            for i in range(min(count, 10)):
                candidate = locator.nth(i)
                try:
                    text = candidate.inner_text(timeout=1000)
                except Exception:
                    continue

                if (
                    "Find a Whole Foods Market near you" in text
                    or "Locate a store" in text
                    or "Search by ZIP code, city or state" in text
                ):
                    return candidate
        except Exception:
            continue

    return None


def fill_store_search_input(scope, page, value: str) -> bool:
    search_locators = [
        scope.get_by_placeholder(re.compile(r"search by zip code, city or state", re.I)),
        scope.locator('input[placeholder*="ZIP code, city or state" i]'),
        scope.locator('input[placeholder*="city or state" i]'),
        scope.locator('input[placeholder*="zip" i]'),
        scope.get_by_role("searchbox"),
        scope.locator('input[type="search"]'),
        scope.locator("input"),
    ]

    for locator in search_locators:
        try:
            count = locator.count()
            for i in range(min(count, 10)):
                candidate = locator.nth(i)

                try:
                    candidate.scroll_into_view_if_needed(timeout=800)
                except Exception:
                    pass

                try:
                    placeholder = (candidate.get_attribute("placeholder") or "").lower()
                    aria_label = (candidate.get_attribute("aria-label") or "").lower()
                    input_type = (candidate.get_attribute("type") or "").lower()
                except Exception:
                    placeholder = ""
                    aria_label = ""
                    input_type = ""

                looks_right = (
                    "zip" in placeholder
                    or "city" in placeholder
                    or "state" in placeholder
                    or "zip" in aria_label
                    or "city" in aria_label
                    or "state" in aria_label
                    or input_type in ("search", "text", "")
                )

                if not looks_right:
                    continue

                try:
                    candidate.click(timeout=1200)
                except Exception:
                    continue

                try:
                    candidate.fill("", timeout=1200)
                except Exception:
                    pass

                try:
                    candidate.type(value, delay=35, timeout=2500)
                except Exception:
                    try:
                        candidate.fill(value, timeout=2500)
                    except Exception:
                        continue

                page.wait_for_timeout(1000)

                try:
                    candidate.press("Enter")
                except Exception:
                    pass

                page.wait_for_timeout(1200)
                return True
        except Exception:
            continue

    return False


def click_columbus_store_result(scope) -> bool:
    result_patterns = [
        re.compile(r"columbus circle", re.I),
        re.compile(r"10 columbus circle", re.I),
    ]

    for pattern in result_patterns:
        try:
            card = scope.locator("li, article, [data-testid], [class*='store']").filter(has_text=pattern).first
            targeted = [
                card.locator(".w-store-finder-store-selector"),
                card.get_by_text(STORE_SELECT_CTA_PATTERN),
                card.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
                card.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
                card.locator('button:has-text("Shop Store")'),
                card.locator('a:has-text("Shop Store")'),
                card.locator('button'),
                card.locator('a'),
            ]
            if click_first_no_wait(targeted, timeout=2400):
                return True
            if click_first_no_wait([card], timeout=2200):
                return True
        except Exception:
            continue

    generic = [
        scope.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
        scope.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
        scope.get_by_text(STORE_SELECT_CTA_PATTERN),
        scope.locator('button:has-text("Shop Store"), button:has-text("Make this my store"), button:has-text("Select store"), button:has-text("Choose store")'),
    ]
    return click_first_no_wait(generic, timeout=2200)


def set_store_via_store_modal_url(page, progress: ProgressBar, start_progress: float = 0.01, end_progress: float = SET_STORE_PROGRESS) -> None:
    total_steps = [
        ("goto", 2500),
        ("search", 2200),
        ("click_store", 1800),
    ]
    total_ms = sum(ms for _, ms in total_steps)
    progressed_ms = 0

    page.goto(STORE_MODAL_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)
    dismiss_popups(page)
    progressed_ms += 2500
    progress.update(start_progress + (end_progress - start_progress) * (progressed_ms / total_ms))

    modal = get_store_modal(page)
    scope = modal or page

    search_ok = fill_store_search_input(scope, page, STORE_SEARCH_TEXT)
    progressed_ms += 2200
    progress.update(start_progress + (end_progress - start_progress) * (progressed_ms / total_ms))

    if not search_ok:
        dump_inputs(scope, "store-modal-scope")
        debug_body(page)
        raise RuntimeError("Could not find or use the store search input on the store modal page.")

    modal = get_store_modal(page)
    scope = modal or page

    made_store = click_columbus_store_result(scope)
    if not made_store:
        dismiss_popups(page)
        page.wait_for_timeout(600)
        modal = get_store_modal(page)
        scope = modal or page
        made_store = click_columbus_store_result(scope)
    if not made_store:
        debug_body(page)
        raise RuntimeError("Could not click the Columbus Circle store CTA.")

    page.wait_for_timeout(1800)
    progressed_ms += 1800
    progress.update(end_progress)


def fast_scroll_to_trigger_next_batch(page) -> bool:
    return page.evaluate(
        """
        () => {
            const cards = document.querySelectorAll('[id^="gridCell-"]');
            if (!cards.length) return false;

            const lastCard = cards[cards.length - 1];
            const rect = lastCard.getBoundingClientRect();
            const currentY = window.scrollY;

            const targetY = Math.max(
                0,
                currentY + rect.top - (window.innerHeight * 0.28)
            );

            window.scrollTo(0, targetY);
            return true;
        }
        """
    )


def discover_all_deals() -> dict:
    captured_batches = []
    recs_by_index = {}
    products_by_asin = {}
    progress = ProgressBar(width=PROGRESS_BAR_WIDTH, initial_eta_seconds=INITIAL_TOTAL_ETA_SECONDS)

    progress.update(0.01)

    with sync_playwright() as p:
        progress.animate_wait(0.01, 0.03, 400, steps=5)

        browser = p.chromium.launch(headless=True)
        progress.animate_wait(0.03, 0.05, 700, steps=6)

        context = browser.new_context()
        progress.animate_wait(0.05, 0.06, 350, steps=4)

        page = context.new_page()
        progress.animate_wait(0.06, 0.07, 300, steps=4)

        def handle_response(response):
            url = response.url
            if "getGridAsins" not in url:
                return

            try:
                req = response.request
                post_data = req.post_data or ""
                body = json.loads(post_data)
            except Exception:
                return

            captured_batches.append({
                "url": url,
                "request_body": body,
            })

            for rec_str in body.get("cardRecommendations", []):
                rec = parse_card_recommendation(rec_str)
                if rec["index"] not in recs_by_index:
                    recs_by_index[rec["index"]] = rec["raw"]

        page.on("response", handle_response)

        set_store_via_store_modal_url(page, progress, start_progress=0.07, end_progress=SET_STORE_PROGRESS)

        page.goto(ALL_DEALS_URL, wait_until="domcontentloaded")
        progress.animate_wait(SET_STORE_PROGRESS, OPEN_DEALS_PROGRESS, 2500, steps=10)

        displayed_scroll_progress = SCROLL_PROGRESS_START

        for i in range(MAX_SCROLL_ROUNDS):
            did_scroll = fast_scroll_to_trigger_next_batch(page)
            if not did_scroll:
                break

            next_target = SCROLL_PROGRESS_START + (
                (SCROLL_PROGRESS_END - SCROLL_PROGRESS_START) * ((i + 1) / MAX_SCROLL_ROUNDS)
            )
            next_target = max(displayed_scroll_progress + 0.002, next_target)

            progress.animate_wait(displayed_scroll_progress, next_target, SCROLL_PAUSE_MS, steps=4)
            displayed_scroll_progress = min(next_target, SCROLL_PROGRESS_END)

        progress.animate_wait(displayed_scroll_progress, FINAL_PARSE_PROGRESS, FINAL_SETTLE_MS, steps=6)

        final_html = page.content()
        parsed_products = parse_all_deals_html(final_html)

        for product in parsed_products:
            asin = product.get("asin")
            if asin:
                products_by_asin[asin] = product

        browser.close()

    progress.finish()

    ordered_recommendations = [recs_by_index[i] for i in sorted(recs_by_index)]
    ordered_products = [products_by_asin[k] for k in sorted(products_by_asin)]

    return {
        "recommendation_count": len(ordered_recommendations),
        "product_count": len(ordered_products),
        "recommendations": ordered_recommendations,
        "products": ordered_products,
        "captured_batches": captured_batches,
    }


if __name__ == "__main__":
    result = discover_all_deals()

    with open("discovered_recommendations.json", "w", encoding="utf-8") as f:
        json.dump(result["recommendations"], f, indent=2, ensure_ascii=False)

    with open("discovered_products.json", "w", encoding="utf-8") as f:
        json.dump(result["products"], f, indent=2, ensure_ascii=False)

    with open("captured_batches.json", "w", encoding="utf-8") as f:
        json.dump(result["captured_batches"], f, indent=2, ensure_ascii=False)
