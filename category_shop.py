import json
import re
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


CATEGORY_URLS = [
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=6506977011&ref=wf_dsk_sn_produce-2ff36",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=371469011&ref=wf_dsk_sn_meat-34676",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=16318751&ref=wf_dsk_sn_bakery-8d206",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=18773724011&ref=wf_dsk_sn_deliprep-2cce6",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=371460011&ref=wf_dsk_sn_dairycheese-339d5",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=6459122011&ref=wf_dsk_sn_frozen-69a3c",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=16322721&ref=wf_dsk_sn_snacks-b27f5",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=18787303011&ref=wf_dsk_sn_pantry-2359e",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=16310231&ref=wf_dsk_sn_Beverages-dfcda",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=18765805011&ref=wf_dsk_sn_household-0537e",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=18774136011&ref=wf_dsk_sn_vitamins-9909c",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=3777891&ref=wf_dsk_sn_personal-72f1a",
    "https://www.wholefoodsmarket.com/fmc/m/30004041?almBrandId=VUZHIFdob2xlIEZvb2Rz&ref=wf_dsk_sn_floral-1824b",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=2619534011&ref=wf_dsk_sn_petcare-b0226",
    "https://www.wholefoodsmarket.com/alm/category/?almBrandId=VUZHIFdob2xlIEZvb2Rz&node=165797011&ref=wf_dsk_sn_babycare2-86457",
]

STORE_MODAL_URL = "https://www.wholefoodsmarket.com/stores?modalView=true"
STORE_SEARCH_TEXT = "Columbus Circle"

MAX_SCROLL_ROUNDS = 180
SCROLL_PAUSE_MS = 450
SETTLE_AFTER_NAV_MS = 2500
SETTLE_AFTER_STORE_SET_MS = 1800
FINAL_SETTLE_MS = 1500


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
    m = re.search(r"Join Prime to buy this item at\s+(\$[\d.,]+(?:/\w+)?)", text)
    if m:
        return m.group(1)
    return text


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


def product_has_deal(product: dict) -> bool:
    return bool(
        product.get("discount")
        or product.get("prime_price")
        or product.get("basis_price")
    )


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
            for i in range(min(count, 12)):
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


def get_store_modal(page):
    marker_phrases = [
        "Find a Whole Foods Market near you",
        "Locate a store",
        "Search by ZIP code, city or state",
        "Search by zip code, city or state",
        "Make this my store",
        "Continue",
    ]

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
                    if not candidate.is_visible(timeout=400):
                        continue
                except Exception:
                    continue

                try:
                    if candidate.locator("#store-finder-search-bar, .wfm-search-bar-input").count() > 0:
                        return candidate
                except Exception:
                    pass

                try:
                    text = candidate.inner_text(timeout=1000)
                except Exception:
                    continue

                if any(phrase in text for phrase in marker_phrases):
                    return candidate
        except Exception:
            continue

    return None


def fill_store_search_input(scope, page, value: str) -> bool:
    search_locators = [
        scope.locator('input#store-finder-search-bar.wfm-search-bar-input'),
        scope.locator('input#store-finder-search-bar'),
        scope.locator('.wfm-search-bar-input'),
        scope.locator('#w-store-finder-search-bar input'),
        scope.locator('#w-store-finder-search-bar [contenteditable="true"]'),
        scope.locator('wfm-search-bar input'),
        scope.locator('wfm-search-bar [role="searchbox"]'),
        scope.locator('wfm-search-bar [role="combobox"]'),
        scope.locator('.wfm-search-bar__wrapper input'),
        scope.locator('.wfm-search-bar input'),
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
                    element_id = (candidate.get_attribute("id") or "").lower()
                    class_name = (candidate.get_attribute("class") or "").lower()
                except Exception:
                    placeholder = ""
                    aria_label = ""
                    input_type = ""
                    element_id = ""
                    class_name = ""

                explicit_store_finder_match = (
                    element_id == "store-finder-search-bar"
                    or "wfm-search-bar-input" in class_name
                )
                modal_search_hint = (
                    "zip" in placeholder
                    or "city" in placeholder
                    or "state" in placeholder
                    or "search by zip code" in placeholder
                    or "zip" in aria_label
                    or "city" in aria_label
                    or "state" in aria_label
                )

                looks_right = (
                    explicit_store_finder_match
                    or modal_search_hint
                )

                if not looks_right:
                    continue

                try:
                    candidate.click(timeout=1200)
                except Exception:
                    try:
                        candidate.click(timeout=1200, force=True)
                    except Exception:
                        try:
                            candidate.locator("..").click(timeout=1200)
                        except Exception:
                            continue

                try:
                    candidate.evaluate("(el) => el.focus()")
                except Exception:
                    pass

                try:
                    candidate.click(timeout=1000, click_count=3)
                except Exception:
                    pass

                try:
                    candidate.press("Meta+A")
                    candidate.press("Backspace")
                except Exception:
                    pass

                try:
                    candidate.fill("", timeout=1200)
                except Exception:
                    pass

                def value_matches() -> bool:
                    try:
                        current_value = candidate.input_value(timeout=500)
                    except Exception:
                        try:
                            current_value = candidate.evaluate(
                                "(el) => el.value || el.getAttribute('value') || el.textContent || ''"
                            )
                        except Exception:
                            current_value = ""

                    return value.lower() in (current_value or "").strip().lower()

                try:
                    candidate.type(value, delay=35, timeout=2500)
                except Exception:
                    try:
                        candidate.fill(value, timeout=2500)
                    except Exception:
                        try:
                            candidate.evaluate(
                                """
                                (el, value) => {
                                    el.focus();
                                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
                                    if (setter) {
                                        setter.call(el, value);
                                    } else {
                                        el.value = value;
                                    }
                                    el.dispatchEvent(new InputEvent('input', { bubbles: true, data: value, inputType: 'insertText' }));
                                    el.dispatchEvent(new Event('change', { bubbles: true }));
                                }
                                """,
                                value,
                            )
                        except Exception:
                            continue

                if not value_matches():
                    try:
                        candidate.click(timeout=1000)
                    except Exception:
                        pass

                    try:
                        page.keyboard.insert_text(value)
                    except Exception:
                        pass

                if not value_matches():
                    continue

                page.wait_for_timeout(900)

                try:
                    candidate.press("Enter")
                except Exception:
                    pass

                try:
                    candidate.evaluate(
                        """
                        (el) => {
                            el.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'Enter', code: 'Enter' }));
                            el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Enter', code: 'Enter' }));
                        }
                        """
                    )
                except Exception:
                    pass

                try:
                    clicked_search = bool(
                        candidate.evaluate(
                            """
                            (el) => {
                                const root = el.closest('#w-store-finder-search-bar, wfm-search-bar, .wfm-search-bar, .wfm-search-bar__wrapper')
                                    || el.parentElement;
                                if (!root) return false;

                                const targets = Array.from(
                                    root.querySelectorAll('#search-icon, [id$="search-icon"], [aria-label="Search"], span[role="button"], button')
                                );

                                for (const target of targets) {
                                    const text = (target.innerText || target.textContent || target.getAttribute('aria-label') || '')
                                        .replace(/\\s+/g, ' ')
                                        .trim()
                                        .toLowerCase();
                                    if (text && text !== 'search') continue;
                                    target.scrollIntoView({ block: "center", inline: "center" });
                                    ["mousedown", "mouseup", "click"].forEach((type) => {
                                        target.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
                                    });
                                    if (typeof target.click === "function") target.click();
                                    return true;
                                }

                                return false;
                            }
                            """
                        )
                    )
                    if clicked_search:
                        page.wait_for_timeout(600)
                except Exception:
                    pass

                page.wait_for_timeout(1200)
                return True
        except Exception:
            continue

    return False


def set_store_via_store_modal_url(page) -> None:
    page.goto(STORE_MODAL_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2200)
    dismiss_popups(page)

    modal = get_store_modal(page)
    scope = modal or page

    search_ok = fill_store_search_input(scope, page, STORE_SEARCH_TEXT)
    if not search_ok:
        raise RuntimeError("Could not find or use the store search input on the store modal page.")

    modal = get_store_modal(page)
    scope = modal or page

    made_store = click_first([
        scope.get_by_role("button", name=re.compile(r"make this my store", re.I)),
        scope.get_by_role("link", name=re.compile(r"make this my store", re.I)),
        scope.get_by_text(re.compile(r"make this my store", re.I)),
        scope.locator("text=Make this my store"),
    ])
    if not made_store:
        raise RuntimeError('Could not click the first "Make this my store".')

    page.wait_for_timeout(SETTLE_AFTER_STORE_SET_MS)


def extract_save_on_targets(page) -> list[dict]:
    targets = page.evaluate(
        """
        () => {
            const out = [];
            const seen = new Set();

            function cleanText(s) {
                return (s || "").replace(/\\s+/g, " ").trim();
            }

            function getHeadingNear(el) {
                let node = el;
                for (let i = 0; i < 10 && node; i++) {
                    const text = cleanText(node.innerText || "");
                    if (/save on/i.test(text)) return text;
                    node = node.parentElement;
                }
                return "";
            }

            function getContextNear(el) {
                let node = el;
                for (let i = 0; i < 8 && node; i++) {
                    const text = cleanText(node.innerText || "");
                    if (text) return text.slice(0, 500);
                    node = node.parentElement;
                }
                return "";
            }

            const elements = Array.from(document.querySelectorAll("a, button"));

            for (const el of elements) {
                const txt = cleanText(el.innerText || el.textContent || "");
                if (!/^(shop all|see all|see more)$/i.test(txt)) continue;

                const sectionText = getHeadingNear(el);
                const href = el.tagName.toLowerCase() === "a" ? el.getAttribute("href") : null;

                const key = `${txt}|${href || ""}|${sectionText}`;
                if (seen.has(key)) continue;
                seen.add(key);

                out.push({
                    text: txt,
                    href: href,
                    section_text: sectionText,
                    context_text: getContextNear(el),
                    dom_index: elements.indexOf(el),
                    tag_name: el.tagName.toLowerCase(),
                });
            }

            return out;
        }
        """
    )

    cleaned = []
    for t in targets:
        context = " ".join([
            t.get("section_text") or "",
            t.get("context_text") or "",
        ]).lower()
        if (
            "save on" in context
            or "deal" in context
            or "discount" in context
            or "prime" in context
            or len(targets) == 1
        ):
            cleaned.append(t)

    if not cleaned:
        cleaned = list(targets)

    cleaned.sort(
        key=lambda t: (
            "save on" not in " ".join([
                (t.get("section_text") or ""),
                (t.get("context_text") or ""),
            ]).lower(),
            "prime" not in " ".join([
                (t.get("section_text") or ""),
                (t.get("context_text") or ""),
            ]).lower(),
        )
    )

    return cleaned


def wait_for_grid_to_appear(page, timeout_ms: int = 7000) -> bool:
    deadline = timeout_ms

    while deadline > 0:
        try:
            if page.locator('[id^="gridCell-"]').count() > 0:
                return True
        except Exception:
            pass

        page.wait_for_timeout(250)
        deadline -= 250

    return False


def click_target_on_current_page(page, target: dict) -> bool:
    try:
        return bool(
            page.evaluate(
                """
                ({ targetText, targetHref, expectedSectionText, expectedContextText }) => {
                    const cleanText = (s) => (s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                    const wantedText = cleanText(targetText || "");
                    const wantedHref = (targetHref || "").trim();
                    const wantedSection = cleanText(expectedSectionText || "");
                    const wantedContext = cleanText(expectedContextText || "");

                    function getHeadingNear(el) {
                        let node = el;
                        for (let i = 0; i < 10 && node; i++) {
                            const text = cleanText(node.innerText || "");
                            if (text.includes("save on")) return text;
                            node = node.parentElement;
                        }
                        return "";
                    }

                    function getContextNear(el) {
                        let node = el;
                        for (let i = 0; i < 8 && node; i++) {
                            const text = cleanText(node.innerText || "");
                            if (text) return text.slice(0, 500);
                            node = node.parentElement;
                        }
                        return "";
                    }

                    const candidates = Array.from(document.querySelectorAll("a, button"));
                    let best = null;
                    let bestScore = -1;

                    for (const el of candidates) {
                        const txt = cleanText(el.innerText || el.textContent || "");
                        if (txt !== wantedText) continue;

                        const href = el.tagName.toLowerCase() === "a" ? (el.getAttribute("href") || "").trim() : "";
                        const section = getHeadingNear(el);
                        const context = getContextNear(el);
                        let score = 0;

                        if (wantedHref && href === wantedHref) score += 5;
                        if (wantedSection && section.includes(wantedSection)) score += 4;
                        if (wantedContext && context.includes(wantedContext.slice(0, 180))) score += 2;
                        if (section.includes("save on")) score += 2;
                        if (context.includes("deal") || context.includes("prime") || context.includes("discount")) score += 1;

                        if (score > bestScore) {
                            best = el;
                            bestScore = score;
                        }
                    }

                    if (!best) return false;

                    best.scrollIntoView({ block: "center", inline: "center" });
                    best.click();
                    return true;
                }
                """,
                {
                    "targetText": target.get("text") or "",
                    "targetHref": target.get("href") or "",
                    "expectedSectionText": target.get("section_text") or "",
                    "expectedContextText": target.get("context_text") or "",
                },
            )
        )
    except Exception:
        return False


def open_shop_all_target(page, target: dict) -> bool:
    href = target.get("href")
    if href:
        try:
            page.goto(urljoin("https://www.wholefoodsmarket.com", href), wait_until="domcontentloaded")
            page.wait_for_timeout(1800)
            return True
        except Exception:
            pass

    if click_target_on_current_page(page, target):
        page.wait_for_timeout(600)
        return True

    for _ in range(12):
        did_scroll = page.evaluate(
            """
            () => {
                const currentY = window.scrollY;
                const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
                const step = Math.max(450, Math.round(window.innerHeight * 0.8));
                const targetY = Math.min(maxY, currentY + step);
                if (targetY <= currentY + 1) return false;
                window.scrollTo(0, targetY);
                return true;
            }
            """
        )
        if not did_scroll:
            break

        page.wait_for_timeout(350)
        dismiss_popups(page)
        if click_target_on_current_page(page, target):
            page.wait_for_timeout(600)
            return True

    return False


def merge_products_from_current_page(page, products_by_asin: dict) -> int:
    html = page.content()
    parsed_products = parse_all_deals_html(html)
    added = 0

    for product in parsed_products:
        asin = product.get("asin")
        if not asin or not product_has_deal(product):
            continue

        if asin not in products_by_asin:
            added += 1
        products_by_asin[asin] = product

    return added


def merge_target_lists(existing_targets: list[dict], new_targets: list[dict]) -> list[dict]:
    seen = {
        (
            (target.get("text") or "").strip().lower(),
            (target.get("href") or "").strip(),
            (target.get("section_text") or "").strip().lower(),
            target.get("dom_index"),
        )
        for target in existing_targets
    }

    for target in new_targets:
        key = (
            (target.get("text") or "").strip().lower(),
            (target.get("href") or "").strip(),
            (target.get("section_text") or "").strip().lower(),
            target.get("dom_index"),
        )
        if key in seen:
            continue
        seen.add(key)
        existing_targets.append(target)

    return existing_targets


def scroll_category_landing_page(page, products_by_asin: dict) -> list[dict]:
    found_targets = []
    merge_products_from_current_page(page, products_by_asin)
    merge_target_lists(found_targets, extract_save_on_targets(page))

    stable_rounds = 0

    for _ in range(12):
        did_scroll = page.evaluate(
            """
            () => {
                const currentY = window.scrollY;
                const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
                const step = Math.max(500, Math.round(window.innerHeight * 0.85));
                const targetY = Math.min(maxY, currentY + step);
                if (targetY <= currentY + 1) return false;
                window.scrollTo(0, targetY);
                return true;
            }
            """
        )
        if not did_scroll:
            break

        page.wait_for_timeout(500)
        dismiss_popups(page)
        before_target_count = len(found_targets)
        before_product_count = len(products_by_asin)

        merge_products_from_current_page(page, products_by_asin)
        merge_target_lists(found_targets, extract_save_on_targets(page))

        if len(found_targets) == before_target_count and len(products_by_asin) == before_product_count:
            stable_rounds += 1
            if stable_rounds >= 3:
                break
        else:
            stable_rounds = 0

    page.wait_for_timeout(300)
    merge_products_from_current_page(page, products_by_asin)
    merge_target_lists(found_targets, extract_save_on_targets(page))

    return found_targets


def click_text_pattern(page, patterns: list[str], timeout: int = 1800) -> bool:
    regex = re.compile(rf"^({'|'.join(patterns)})$", re.I)

    clicked = click_first([
        page.get_by_role("button", name=regex),
        page.get_by_role("link", name=regex),
        page.get_by_role("checkbox", name=regex),
        page.get_by_text(regex),
        page.locator("label").filter(has_text=regex),
    ], timeout=timeout)
    if clicked:
        return True

    try:
        return bool(
            page.evaluate(
                """
                (patterns) => {
                    const regexes = patterns.map((pattern) => new RegExp(`^${pattern}$`, "i"));

                    function textOf(el) {
                        return (el.innerText || el.textContent || el.getAttribute("aria-label") || "")
                            .replace(/\\s+/g, " ")
                            .trim();
                    }

                    function isVisible(el) {
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== "hidden" && style.display !== "none";
                    }

                    const candidates = Array.from(document.querySelectorAll("button, a, label, [role='button'], [role='link'], [role='checkbox'], [aria-label]"));

                    for (const el of candidates) {
                        const text = textOf(el);
                        if (!text || !isVisible(el)) continue;
                        if (!regexes.some((regex) => regex.test(text))) continue;
                        el.scrollIntoView({ block: "center", inline: "center" });
                        el.click();
                        return true;
                    }

                    return false;
                }
                """,
                patterns,
            )
        )
    except Exception:
        return False


def apply_all_discounts_filter(page) -> bool:
    dismiss_popups(page)

    if click_text_pattern(page, [r"all discounts"], timeout=1500):
        page.wait_for_timeout(1200)
        click_text_pattern(page, [r"apply", r"show results", r"view results", r"done"], timeout=1200)
        page.wait_for_timeout(1200)
        return True

    opened_filters = click_text_pattern(page, [r"filters?", r"all filters?", r"refine"], timeout=1500)
    if not opened_filters:
        return False

    page.wait_for_timeout(800)

    clicked_discount = click_text_pattern(page, [r"all discounts"], timeout=1500)
    if not clicked_discount:
        return False

    page.wait_for_timeout(800)
    click_text_pattern(page, [r"apply", r"show results", r"view results", r"done"], timeout=1500)
    page.wait_for_timeout(1200)
    return True


def fast_scroll_to_trigger_next_batch(page) -> bool:
    return page.evaluate(
        """
        () => {
            const cards = Array.from(document.querySelectorAll('[id^="gridCell-"]')).filter((card) => {
                const rect = card.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
            });
            if (!cards.length) return false;

            const lastCard = cards[cards.length - 1];
            const rect = lastCard.getBoundingClientRect();

            function findScrollableAncestor(node) {
                let current = node;

                while (current && current !== document.body) {
                    const style = window.getComputedStyle(current);
                    const overflowY = style.overflowY || "";
                    if (
                        /(auto|scroll)/i.test(overflowY)
                        && current.scrollHeight > current.clientHeight + 100
                    ) {
                        return current;
                    }
                    current = current.parentElement;
                }

                return document.scrollingElement || document.documentElement;
            }

            const scroller = findScrollableAncestor(lastCard.parentElement || lastCard);

            if (
                scroller === document.body
                || scroller === document.documentElement
                || scroller === document.scrollingElement
            ) {
                const currentY = window.scrollY;
                const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
                const step = Math.max(300, Math.round(window.innerHeight * 0.7));
                const targetY = Math.min(
                    maxY,
                    Math.max(currentY + step, currentY + rect.top - (window.innerHeight * 0.35))
                );
                if (targetY <= currentY + 1) return false;
                window.scrollTo(0, targetY);
                return true;
            }

            const scrollerRect = scroller.getBoundingClientRect();
            const currentTop = scroller.scrollTop;
            const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
            const step = Math.max(260, Math.round(scroller.clientHeight * 0.7));
            const cardOffset = currentTop + rect.top - scrollerRect.top;
            const targetTop = Math.min(
                maxTop,
                Math.max(currentTop + step, cardOffset - (scroller.clientHeight * 0.35))
            );

            if (targetTop <= currentTop + 1) return false;

            scroller.scrollTo(0, targetTop);
            return true;
        }
        """
    )


def scroll_and_collect_from_current_page(page, products_by_asin: dict) -> None:
    wait_for_grid_to_appear(page, timeout_ms=8000)
    merge_products_from_current_page(page, products_by_asin)

    last_card_count = -1
    stable_rounds = 0

    for _ in range(MAX_SCROLL_ROUNDS):
        did_scroll = fast_scroll_to_trigger_next_batch(page)
        if not did_scroll:
            break

        page.wait_for_timeout(SCROLL_PAUSE_MS)
        current_card_count = page.locator('[id^="gridCell-"]').count()
        merge_products_from_current_page(page, products_by_asin)

        if current_card_count == last_card_count:
            stable_rounds += 1
            page.wait_for_timeout(250)
            if stable_rounds >= 4:
                break
        else:
            stable_rounds = 0

        last_card_count = current_card_count

    page.wait_for_timeout(FINAL_SETTLE_MS)
    merge_products_from_current_page(page, products_by_asin)


def scrape_category_page(page, category_url: str, products_by_asin: dict, visited_category_urls: list, visited_shop_all_urls: list) -> None:
    print(f"\nOpening category page: {category_url}")
    page.goto(category_url, wait_until="domcontentloaded")
    page.wait_for_timeout(SETTLE_AFTER_NAV_MS)
    dismiss_popups(page)

    visited_category_urls.append(page.url)

    targets = scroll_category_landing_page(page, products_by_asin)
    print(f"Found {len(targets)} Save on target(s) on this category page.")
    print(f"  Unique deal products seen on landing page so far: {len(products_by_asin)}")

    for idx in range(len(targets)):
        print(f"  Opening Save on target {idx + 1}/{len(targets)}: {targets[idx].get('text')}")
        target_href = targets[idx].get("href")
        needs_category_page = not target_href

        if needs_category_page and (idx > 0 or page.url != category_url):
            page.goto(category_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1800)
            dismiss_popups(page)
            merge_products_from_current_page(page, products_by_asin)

        before_url = page.url
        ok = open_shop_all_target(page, targets[idx])
        if not ok:
            print("   Could not open this Save on target.")
            continue

        wait_for_grid_to_appear(page, timeout_ms=8000)
        if page.url == before_url:
            page.wait_for_timeout(1800)
        else:
            page.wait_for_timeout(2200)

        filter_applied = False
        if page.url != before_url:
            filter_applied = apply_all_discounts_filter(page)
            if filter_applied:
                wait_for_grid_to_appear(page, timeout_ms=8000)

        dismiss_popups(page)

        visited_shop_all_urls.append(page.url)
        scroll_and_collect_from_current_page(page, products_by_asin)
        if filter_applied:
            print("   Applied All discounts filter on routed page.")
        print(f"   Total unique products so far: {len(products_by_asin)}")


def discover_category_shop_all_deals() -> dict:
    products_by_asin = {}
    visited_category_urls = []
    visited_shop_all_urls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=40)
        context = browser.new_context(viewport={"width": 1400, "height": 1100})
        page = context.new_page()

        set_store_via_store_modal_url(page)

        for url in CATEGORY_URLS:
            try:
                scrape_category_page(
                    page=page,
                    category_url=url,
                    products_by_asin=products_by_asin,
                    visited_category_urls=visited_category_urls,
                    visited_shop_all_urls=visited_shop_all_urls,
                )
            except Exception as e:
                print(f"Error on category page: {url}")
                print(f"  {e}")

        browser.close()

    ordered_products = [products_by_asin[k] for k in sorted(products_by_asin)]

    return {
        "category_url_count": len(CATEGORY_URLS),
        "visited_category_urls": visited_category_urls,
        "visited_shop_all_urls": visited_shop_all_urls,
        "product_count": len(ordered_products),
        "products": ordered_products,
    }


if __name__ == "__main__":
    result = discover_category_shop_all_deals()

    with open("category_shop_all_products.json", "w", encoding="utf-8") as f:
        json.dump(result["products"], f, indent=2, ensure_ascii=False)

    with open("category_shop_all_report.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "category_url_count": result["category_url_count"],
                "visited_category_urls": result["visited_category_urls"],
                "visited_shop_all_urls": result["visited_shop_all_urls"],
                "product_count": result["product_count"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"\nDone. Unique products collected: {result['product_count']}")
