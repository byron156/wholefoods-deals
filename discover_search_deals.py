import json
import os
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urljoin

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from category_shop import (
    STORE_SEARCH_TEXT,
    click_first,
    dismiss_popups,
    fill_store_search_input,
    get_store_modal,
    parse_all_deals_html,
)


BASE_DEALS_RH = "p_n_deal_type:23566065011"
SEARCH_DEALS_BASE_URL = "https://www.wholefoodsmarket.com/grocery/search"
STORE_MODAL_URL = "https://www.wholefoodsmarket.com/stores?modalView=true"
SEARCH_DEALS_URL = (
    f"{SEARCH_DEALS_BASE_URL}?{urlencode({'k': '', 'rh': BASE_DEALS_RH, 's': 'relevanceblender'})}"
)
SEARCH_DEALS_PARTIAL_PRODUCTS_FILE = Path(__file__).with_name("search_deals_products.partial.json")
SEARCH_DEALS_PARTIAL_REPORT_FILE = Path(__file__).with_name("search_deals_report.partial.json")
STORE_SELECT_CTA_PATTERN = re.compile(
    r"make this my store|shop store|select store|choose store|set as my store",
    re.I,
)
GOOGLE_CHROME_EXECUTABLE = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
HEADLESS_CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""

MAX_ACTION_ROUNDS = 240
INITIAL_PAGE_SETTLE_MS = 2000
SCROLL_PAUSE_MS = 700
LOAD_MORE_SETTLE_MS = 1800
FINAL_SETTLE_MS = 1500
POST_STORE_SET_WAIT_MS = 2500
BASE_SORT_RUNS = [
    ("Relevance", "relevanceblender"),
    ("Price: Low to High", "price-asc-rank"),
    ("Price: High to Low", "price-desc-rank"),
    ("Newest Arrivals", "date-desc-rank"),
    ("Get it fast", "get-it-fast-rank"),
    ("Most purchased", "most-purchased-rank"),
    ("Low prices", "low-prices-rank"),
    ("Best Sellers", "exact-aware-popularity-rank"),
]
FAST_BASE_SORT_RUNS = [
    ("Relevance", "relevanceblender"),
    ("Price: Low to High", "price-asc-rank"),
    ("Price: High to Low", "price-desc-rank"),
    ("Newest Arrivals", "date-desc-rank"),
]
FILTER_RUNS = [
    {
        "filter_label": "Amazon Brands",
        "rh_values": ["p_n_g-1001321510111:24677333011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "FSA or HSA Eligible",
        "rh_values": ["p_n_hba_program:17904039011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Any Feature",
        "rh_values": ["p_n_cpf_labels:121136630011"],
        "sorts": [
            ("Price: Low to High", "price-asc-rank"),
            ("Price: High to Low", "price-desc-rank"),
        ],
    },
    {
        "filter_label": "Biodiversity",
        "rh_values": ["p_n_cpf_labels:116845691011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Carbon Impact",
        "rh_values": ["p_n_cpf_labels:116845688011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Farming Practices",
        "rh_values": ["p_n_cpf_labels:121191385011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Forestry Practices",
        "rh_values": ["p_n_cpf_labels:116845687011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Manufacturing Practices",
        "rh_values": ["p_n_cpf_labels:116845690011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Organic Content",
        "rh_values": ["p_n_cpf_labels:116845684011"],
        "sorts": [
            ("Price: Low to High", "price-asc-rank"),
            ("Price: High to Low", "price-desc-rank"),
        ],
    },
    {
        "filter_label": "Packaging Efficiency",
        "rh_values": ["p_n_cpf_labels:116845682011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Safer Chemicals",
        "rh_values": ["p_n_cpf_labels:116845683011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
    {
        "filter_label": "Worker Wellbeing",
        "rh_values": ["p_n_cpf_labels:116845686011"],
        "sorts": [("Relevance", "relevanceblender")],
    },
]


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def print_search_progress(completed: int, total: int, started_at: float, products_count: int, status: str) -> None:
    width = 24
    ratio = completed / total if total else 1
    filled = min(width, max(0, round(width * ratio)))
    bar = "#" * filled + "-" * (width - filled)
    elapsed = time.monotonic() - started_at
    eta = "--"

    if completed > 0 and completed < total:
        eta = format_duration((elapsed / completed) * (total - completed))
    elif completed >= total:
        eta = "0s"

    print(
        f"[search] [{bar}] {completed}/{total} "
        f"elapsed={format_duration(elapsed)} eta={eta} products={products_count} | {status}"
    )


def wait_for_store_iframe(page, timeout_ms: int = 10000):
    remaining = timeout_ms

    while remaining > 0:
        try:
            if page.locator('iframe[title="stores-modal"]').count() > 0:
                return page.frame_locator('iframe[title="stores-modal"]')
        except Exception:
            pass

        page.wait_for_timeout(250)
        remaining -= 250

    return None


def wait_for_store_modal_to_disappear(page, timeout_ms: int = 10000) -> bool:
    remaining = timeout_ms

    while remaining > 0:
        if get_store_modal(page) is None:
            return True

        page.wait_for_timeout(300)
        remaining -= 300

    return get_store_modal(page) is None


def wait_for_store_iframe_text(page, pattern: str, timeout_ms: int = 12000) -> bool:
    iframe = wait_for_store_iframe(page, timeout_ms=timeout_ms)
    scope = iframe or page

    remaining = timeout_ms
    matcher = re.compile(pattern, re.I)

    while remaining > 0:
        try:
            body = scope.locator("body")
            text = body.inner_text(timeout=1200)
            if matcher.search(text):
                return True
        except Exception:
            pass

        page.wait_for_timeout(300)
        remaining -= 300

    return False


def fill_store_search_input_in_iframe(page, value: str) -> bool:
    iframe = wait_for_store_iframe(page, timeout_ms=10000)
    if iframe is None:
        return fill_store_search_input(page, page, value)

    input_box = iframe.locator("#store-finder-search-bar")

    try:
        input_box.wait_for(state="visible", timeout=5000)
    except Exception:
        return False

    try:
        input_box.click(timeout=1800)
    except Exception:
        try:
            input_box.click(timeout=1800, force=True)
        except Exception:
            return False

    try:
        input_box.fill("", timeout=1800)
    except Exception:
        pass

    try:
        input_box.press("Meta+A")
        input_box.press("Backspace")
    except Exception:
        pass

    typed = False
    try:
        input_box.fill(value, timeout=2500)
        typed = True
    except Exception:
        try:
            input_box.type(value, delay=35, timeout=2500)
            typed = True
        except Exception:
            try:
                input_box.evaluate(
                    """
                    (el, nextValue) => {
                        el.focus();
                        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
                        if (setter) {
                            setter.call(el, nextValue);
                        } else {
                            el.value = nextValue;
                        }
                        el.dispatchEvent(new InputEvent("input", { bubbles: true, data: nextValue, inputType: "insertText" }));
                        el.dispatchEvent(new Event("change", { bubbles: true }));
                    }
                    """,
                    value,
                )
                typed = True
            except Exception:
                typed = False

    if not typed:
        return False

    try:
        current_value = input_box.input_value(timeout=1000)
    except Exception:
        current_value = ""

    if value.lower() not in current_value.lower():
        try:
            input_box.click(timeout=1200)
            page.keyboard.insert_text(value)
        except Exception:
            pass

    try:
        input_box.press("Enter")
    except Exception:
        try:
            input_box.evaluate(
                """
                (el) => {
                    el.dispatchEvent(new KeyboardEvent("keydown", { bubbles: true, key: "Enter", code: "Enter" }));
                    el.dispatchEvent(new KeyboardEvent("keyup", { bubbles: true, key: "Enter", code: "Enter" }));
                }
                """
            )
        except Exception:
            return False

    page.wait_for_timeout(1800)
    return True


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip().lower()


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


def click_make_this_my_store_for_columbus(page) -> bool:
    iframe = wait_for_store_iframe(page, timeout_ms=5000)
    scope = iframe or page

    try:
        clicked = bool(
            scope.locator("body").evaluate(
                """
                (body) => {
                    const controls = Array.from(body.querySelectorAll('a, button, input, [role="button"]'));
                    const phoneIndex = controls.findIndex((el) => {
                        const href = el.getAttribute('href') || '';
                        const text = el.innerText || el.textContent || '';
                        return href.includes('823-9600') || text.includes('823-9600');
                    });
                    if (phoneIndex < 0) return false;

                    for (let index = phoneIndex + 1; index < Math.min(controls.length, phoneIndex + 4); index += 1) {
                        const candidate = controls[index];
                        const labelId = candidate.getAttribute('aria-labelledby');
                        const label = labelId ? body.querySelector(`#${CSS.escape(labelId)}`) : null;
                        const text = `${candidate.innerText || ''} ${candidate.textContent || ''} ${candidate.value || ''} ${label ? (label.innerText || label.textContent || '') : ''}`.toLowerCase();
                        if (!/(shop store|make this my store|select store|choose store)/i.test(text)) continue;

                        candidate.scrollIntoView({ block: "center", inline: "center" });
                        ["mousedown", "mouseup", "click"].forEach((type) => {
                            candidate.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
                        });
                        if (typeof candidate.click === "function") candidate.click();
                        return true;
                    }
                    return false;
                }
                """
            )
        )
        if clicked:
            return True
    except Exception:
        pass

    columbus_card = scope.locator("li, article, [data-testid], [class*='store']").filter(
        has_text=re.compile(r"columbus circle", re.I)
    ).first

    targeted_locators = [
        columbus_card.locator(".w-store-finder-store-selector"),
        columbus_card.get_by_text(STORE_SELECT_CTA_PATTERN),
        columbus_card.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
        columbus_card.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
        columbus_card.locator('button:has-text("Shop Store")'),
        columbus_card.locator('a:has-text("Shop Store")'),
        columbus_card.locator('button'),
        columbus_card.locator('a'),
    ]

    if click_first_no_wait(targeted_locators, timeout=2400):
        return True

    generic_locators = [
        scope.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
        scope.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
        scope.get_by_text(STORE_SELECT_CTA_PATTERN),
        scope.locator('button:has-text("Shop Store"), button:has-text("Make this my store"), button:has-text("Select store"), button:has-text("Choose store")'),
    ]
    return click_first_no_wait(generic_locators, timeout=2200)


def get_selected_store_text(page) -> str:
    locators = [
        page.locator('button[aria-label="Select a store"]'),
        page.locator('button[data-csa-c-content-id="Whole Foods Market"]'),
        page.locator('button[aria-label="See store details"]'),
        page.locator('button[data-csa-c-content-id^="My Store"]'),
        page.locator('button:has-text("Pickup at")'),
        page.locator('button:has-text("Delivery at")'),
    ]

    collected = []

    for locator in locators:
        try:
            count = locator.count()
            for i in range(min(count, 6)):
                candidate = locator.nth(i)
                try:
                    if not candidate.is_visible(timeout=300):
                        continue
                except Exception:
                    continue

                parts = []
                try:
                    parts.append(candidate.inner_text(timeout=1000))
                except Exception:
                    pass
                try:
                    parts.append(candidate.text_content(timeout=1000) or "")
                except Exception:
                    pass
                try:
                    parts.append(candidate.get_attribute("aria-label") or "")
                except Exception:
                    pass
                try:
                    parts.append(candidate.get_attribute("data-csa-c-content-id") or "")
                except Exception:
                    pass

                for part in parts:
                    cleaned = " ".join((part or "").split())
                    if cleaned and cleaned not in collected:
                        collected.append(cleaned)
        except Exception:
            continue

    return " | ".join(collected)


def scroll_store_modal_to_bottom(page, timeout_ms: int = 4000) -> bool:
    modal = wait_for_store_modal(page, timeout_ms=timeout_ms)
    if modal is None:
        return False

    try:
        modal.evaluate(
            """
            (root) => {
                const nodes = [root, ...root.querySelectorAll('*')];
                for (const node of nodes) {
                    if (node.scrollHeight > node.clientHeight + 20) {
                        node.scrollTop = node.scrollHeight;
                    }
                }
                root.scrollTop = root.scrollHeight;
            }
            """
        )
        return True
    except Exception:
        return False


def wait_for_continue_enabled(page, timeout_ms: int = 8000) -> bool:
    remaining = timeout_ms

    while remaining > 0:
        locators = [
            page.get_by_role("button", name=re.compile(r"^continue$", re.I)),
            page.get_by_text(re.compile(r"^continue$", re.I)),
        ]

        for locator in locators:
            try:
                count = locator.count()
                for i in range(min(count, 6)):
                    candidate = locator.nth(i)
                    try:
                        if not candidate.is_visible(timeout=300):
                            continue
                    except Exception:
                        continue

                    try:
                        disabled_attr = candidate.get_attribute("disabled")
                        aria_disabled = (candidate.get_attribute("aria-disabled") or "").lower()
                    except Exception:
                        disabled_attr = None
                        aria_disabled = ""

                    if disabled_attr is None and aria_disabled != "true":
                        return True
            except Exception:
                continue

        page.wait_for_timeout(300)
        remaining -= 300

    return False


def click_continue_in_store_modal(page) -> bool:
    scroll_store_modal_to_bottom(page, timeout_ms=3000)
    page.wait_for_timeout(600)
    wait_for_continue_enabled(page, timeout_ms=8000)

    return click_first_no_wait([
        page.get_by_role("button", name=re.compile(r"^continue$", re.I)),
        page.get_by_text(re.compile(r"^continue$", re.I)),
    ], timeout=2200)


def click_close_in_store_modal(page) -> bool:
    return click_first([
        page.get_by_role("button", name=re.compile(r"^close$", re.I)),
        page.get_by_text(re.compile(r"^close$", re.I)),
        page.locator('button:has-text("Close")'),
    ], timeout=2200)


def click_center_of_viewport(page) -> bool:
    try:
        viewport = page.viewport_size or {}
        width = viewport.get("width", 1440)
        height = viewport.get("height", 1100)
        page.mouse.click(width / 2, height / 2)
        return True
    except Exception:
        return False


def wait_for_selected_store_text(page, pattern: str, timeout_ms: int = 8000) -> bool:
    remaining = timeout_ms
    matcher = re.compile(pattern, re.I)

    while remaining > 0:
        selected_store = get_selected_store_text(page)
        if selected_store and (
            matcher.search(selected_store)
            or matcher.search(normalize_text(selected_store))
        ):
            return True

        page.wait_for_timeout(300)
        remaining -= 300

    return False


def wait_for_store_launcher(page, timeout_ms: int = 8000) -> bool:
    remaining = timeout_ms
    selectors = [
        'button[aria-label="Select a store"]',
        'button[data-csa-c-content-id="Whole Foods Market"]',
        'button[aria-label="See store details"]',
        'button[data-csa-c-content-id^="My Store"]',
        'button:has-text("Pickup at")',
        'button:has-text("Delivery at")',
        'button:has-text("Select a store")',
        'button:has-text("Update location")',
        'button:has-text("Find a store")',
        'a[aria-label="Select a store"]',
    ]

    while remaining > 0:
        for selector in selectors:
            try:
                locator = page.locator(selector)
                if locator.count() <= 0:
                    continue

                for i in range(min(locator.count(), 6)):
                    candidate = locator.nth(i)
                    try:
                        if candidate.is_visible(timeout=300):
                            return True
                    except Exception:
                        continue

                if locator.first.is_visible(timeout=300):
                    return True
            except Exception:
                pass

        page.wait_for_timeout(250)
        remaining -= 250

    return False


def wait_for_grid_to_appear(page, timeout_ms: int = 10000) -> bool:
    remaining = timeout_ms

    while remaining > 0:
        try:
            if (
                page.locator('[id^="gridCell-"]').count() > 0
                or page.locator('a[data-csa-c-type="productTile"][href*="/grocery/product/"]').count() > 0
            ):
                return True
        except Exception:
            pass

        page.wait_for_timeout(250)
        remaining -= 250

    return False


def current_rendered_product_count(page) -> int:
    try:
        tile_count = page.locator('a[data-csa-c-type="productTile"][href*="/grocery/product/"]').count()
        if tile_count > 0:
            return tile_count
    except Exception:
        pass

    try:
        return page.locator('[id^="gridCell-"]').count()
    except Exception:
        return 0


def wait_for_results_growth(page, previous_height: int, previous_tile_count: int, timeout_ms: int = 9000) -> bool:
    remaining = timeout_ms

    while remaining > 0:
        try:
            current_height = page.evaluate("() => document.documentElement.scrollHeight")
        except Exception:
            current_height = previous_height

        current_tile_count = current_rendered_product_count(page)

        if current_height > previous_height or current_tile_count > previous_tile_count:
            return True

        page.wait_for_timeout(350)
        remaining -= 350

    return False


def build_search_url(sort_rank: str, extra_rh_values: Optional[list[str]] = None) -> str:
    rh_values = [BASE_DEALS_RH]
    if extra_rh_values:
        rh_values.extend(extra_rh_values)

    return f"{SEARCH_DEALS_BASE_URL}?{urlencode({'k': '', 'rh': ','.join(rh_values), 's': sort_rank})}"


def goto_search_url(page, target_url: str, run_label: str, attempts: int = 2) -> None:
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            return
        except PlaywrightError as exc:
            last_error = exc
            if attempt >= attempts:
                break

            print(f"{run_label}: navigation timed out/failed on attempt {attempt}; retrying.")
            page.wait_for_timeout(3000)

    raise RuntimeError(f'Could not open products for the run "{run_label}": {last_error}')


def write_search_partial_checkpoint(products_by_asin: dict, captured_batch_urls: list, sort_runs_summary: list) -> None:
    ordered_products = [products_by_asin[k] for k in sorted(products_by_asin)]

    with open(SEARCH_DEALS_PARTIAL_PRODUCTS_FILE, "w", encoding="utf-8") as f:
        json.dump(ordered_products, f, indent=2, ensure_ascii=False)

    with open(SEARCH_DEALS_PARTIAL_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "search_url": SEARCH_DEALS_URL,
                "product_count": len(ordered_products),
                "network_batch_count": len(captured_batch_urls),
                "sort_runs": sort_runs_summary,
                "partial": True,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )


def open_search_run(page, run_label: str, sort_label: str, sort_rank: str, extra_rh_values: Optional[list[str]] = None) -> None:
    target_url = build_search_url(sort_rank, extra_rh_values=extra_rh_values)
    print(f"Opening run: {run_label} ({sort_label})")
    print(f"Run URL: {target_url}")

    last_render_error = None

    for render_attempt in range(1, 4):
        if render_attempt > 1:
            print(f'{run_label}: products did not render on attempt {render_attempt - 1}; retrying the run URL.')
            page.wait_for_timeout(3000)

        goto_search_url(page, target_url, run_label, attempts=2)
        page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
        dismiss_popups(page)

        if not wait_for_selected_store_text(page, r"columbus\s+circle", timeout_ms=5000):
            print(f"{run_label}: store check failed after navigation; retrying the store modal flow.")
            set_store_from_search_page(page)
            goto_search_url(page, target_url, run_label, attempts=2)
            page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
            dismiss_popups(page)

            if not wait_for_selected_store_text(page, r"columbus\s+circle", timeout_ms=6000):
                last_render_error = (
                    f'The page did not keep "Columbus Circle" selected while opening the run "{run_label}".'
                )
                continue

        if wait_for_grid_to_appear(page, timeout_ms=20000):
            return

        last_render_error = f'Products did not render for the run "{run_label}".'
        try:
            debug_body(page)
        except Exception:
            pass

    raise RuntimeError(last_render_error or f'Products did not render for the run "{run_label}".')


def format_money(value):
    if value is None:
        return None
    return f"${value:.2f}"


def format_unit_price(unit_price: Optional[dict]) -> Optional[str]:
    if not unit_price:
        return None

    amount = unit_price.get("priceAmount")
    base_unit = unit_price.get("baseUnit")
    if amount is None:
        return None

    rendered = format_money(amount)
    if base_unit:
        return f"{rendered}/{base_unit}"
    return rendered


def parse_next_data_products(html: str) -> list[dict]:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
        html,
        flags=re.S,
    )
    if not match:
        return []

    try:
        data = json.loads(match.group(1))
    except Exception:
        return []

    products_info = (
        data.get("props", {})
        .get("pageProps", {})
        .get("searchResults", {})
        .get("productsInfo", [])
    )

    products = []

    for item in products_info:
        asin = item.get("asin")
        if not asin:
            continue

        offer_details = item.get("offerDetails") or {}
        price = offer_details.get("price") or {}
        prime_benefit = price.get("primeBenefit") or {}

        discount = None
        savings = price.get("savings") or {}
        if savings.get("percentSavings"):
            discount = savings.get("percentSavings")

        image = None
        images = item.get("productImages") or []
        if images:
            image = images[0]

        url = None
        if asin:
            url = f"https://www.wholefoodsmarket.com/product/{asin}"

        products.append({
            "asin": asin,
            "name": item.get("name"),
            "image": image,
            "url": url,
            "current_price": format_money(price.get("priceAmount")),
            "basis_price": format_money(price.get("basisPriceAmount")),
            "prime_price": format_money(prime_benefit.get("priceAmount")),
            "discount": discount,
            "unit_price": format_unit_price(offer_details.get("unitPrice")),
            "availability": item.get("availability"),
        })

    return products


def parse_rendered_product_tiles(html: str) -> list[dict]:
    products = []

    try:
        from bs4 import BeautifulSoup
    except Exception:
        return products

    soup = BeautifulSoup(html, "html.parser")

    for link in soup.select('a[data-csa-c-type="productTile"][href*="/grocery/product/"]'):
        href = link.get("href") or ""
        if not href:
            continue

        card = link.parent if getattr(link, "parent", None) is not None else None
        if card is None or getattr(card, "name", None) != "div":
            continue

        asin = link.get("data-csa-c-content-id")
        if not asin:
            match = re.search(r"-([A-Z0-9]{10})(?:\?|$)", href, re.I)
            asin = match.group(1).upper() if match else None

        top_section = None
        for child in link.find_all("div", recursive=False):
            classes = child.get("class") or []
            if "flex" in classes and "flex-col" in classes and "gap-2" in classes:
                top_section = child
                break

        image = None
        brand = None
        name = None

        if top_section is not None:
            img = top_section.find("img")
            if img:
                image = img.get("src")

            brand_el = top_section.find("span", class_=lambda c: c and "bds--body-2" in c)
            if brand_el:
                brand = brand_el.get_text(" ", strip=True)

            name_el = top_section.find("span", class_=lambda c: c and "bds--heading-5" in c)
            if name_el:
                name = name_el.get_text(" ", strip=True)

        if not name:
            continue

        variation_text = None
        variation_el = link.find("span", class_=lambda c: c and "bds--body-6" in c)
        if variation_el:
            variation_text = variation_el.get_text(" ", strip=True)

        discount = None
        discount_el = link.find("span", string=lambda s: s and ("off" in s.lower() or "deal" in s.lower()))
        if discount_el:
            discount = discount_el.get_text(" ", strip=True)

        prime_price = None
        prime_el = link.find("p", string=lambda s: s and "Join Prime to buy this item at" in s)
        if prime_el:
            prime_text = prime_el.get_text(" ", strip=True)
            match = re.search(r"(\$[\d.,]+(?:/\w+)?)", prime_text)
            if match:
                prime_price = match.group(1)

        current_price = None
        basis_price = None
        unit_price = None

        price_rows = link.find_all("div", class_=lambda c: c and "flex" in c and "flex-wrap" in c and "gap-2" in c)
        if price_rows:
            price_row = price_rows[-1]
            current_el = price_row.find("span", class_=lambda c: c and "bds--heading-5" in c)
            if current_el:
                current_price = current_el.get_text(" ", strip=True)
                if "/" in current_price:
                    unit_price = current_price

            basis_el = price_row.find("span", class_=lambda c: c and "line-through" in c)
            if basis_el:
                basis_price = basis_el.get_text(" ", strip=True)

        if not unit_price and current_price and "/" in current_price:
            unit_price = current_price

        products.append({
            "asin": asin,
            "name": name,
            "brand": brand,
            "variation": variation_text,
            "image": image,
            "url": urljoin("https://www.wholefoodsmarket.com", href),
            "current_price": current_price,
            "basis_price": basis_price,
            "prime_price": prime_price,
            "discount": discount,
            "unit_price": unit_price,
        })

    return products


def merge_product(existing: Optional[dict], new_product: dict) -> dict:
    if not existing:
        return dict(new_product)

    merged = dict(existing)
    for key, value in new_product.items():
        if value in (None, "", []):
            continue
        if key not in merged or merged.get(key) in (None, "", []):
            merged[key] = value
    return merged


def merge_products_from_current_page(page, products_by_asin: dict) -> int:
    html = page.content()
    parsed_products = parse_all_deals_html(html)
    parsed_products.extend(parse_rendered_product_tiles(html))
    parsed_products.extend(parse_next_data_products(html))
    added = 0

    for product in parsed_products:
        asin = product.get("asin")
        if not asin:
            continue

        if asin not in products_by_asin:
            added += 1
        products_by_asin[asin] = merge_product(products_by_asin.get(asin), product)

    return added


def scroll_page_down(page) -> bool:
    return bool(
        page.evaluate(
            """
            () => {
                const currentY = window.scrollY;
                const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
                const step = Math.max(700, Math.round(window.innerHeight * 0.92));
                const targetY = Math.min(maxY, currentY + step);
                if (targetY <= currentY + 1) return false;
                window.scrollTo(0, targetY);
                return true;
            }
            """
        )
    )


def scroll_to_top(page) -> None:
    try:
        page.evaluate("() => window.scrollTo({ top: 0, behavior: 'instant' })")
    except Exception:
        page.evaluate("() => window.scrollTo(0, 0)")

    page.wait_for_timeout(900)
    dismiss_popups(page)


def click_load_more(page) -> bool:
    candidates = [
        page.get_by_role("button", name=re.compile(r"^load more$", re.I)),
        page.get_by_role("link", name=re.compile(r"^load more$", re.I)),
        page.get_by_text(re.compile(r"^load more$", re.I)),
        page.locator("button:text-is('Load more')"),
        page.locator("a:text-is('Load more')"),
    ]

    for locator in candidates:
        try:
            count = locator.count()
            for i in range(min(count, 6)):
                button = locator.nth(i)
                try:
                    disabled = (button.get_attribute("disabled") or "").lower()
                    aria_disabled = (button.get_attribute("aria-disabled") or "").lower()
                    if disabled or aria_disabled == "true":
                        continue
                except Exception:
                    pass

                try:
                    button.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass

                try:
                    button.click(timeout=1600)
                    return True
                except Exception:
                    try:
                        button.click(timeout=1600, force=True)
                        return True
                    except Exception:
                        continue
        except Exception:
            continue

    try:
        return bool(
            page.evaluate(
                """
                () => {
                    function cleanText(s) {
                        return (s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                    }

                    function isVisible(el) {
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
                    }

                    const elements = Array.from(document.querySelectorAll("button, a, [role='button'], [role='link']"));

                    for (const el of elements) {
                        const text = cleanText(el.innerText || el.textContent || el.getAttribute("aria-label") || "");
                        if (text !== "load more") continue;
                        if (!isVisible(el)) continue;
                        if (el.hasAttribute("disabled") || el.getAttribute("aria-disabled") === "true") continue;
                        el.scrollIntoView({ block: "center", inline: "center" });
                        el.click();
                        return true;
                    }

                    return false;
                }
                """
            )
        )
    except Exception:
        return False


def open_sort_menu(page) -> bool:
    sort_button_candidates = [
        page.get_by_role("button", name=re.compile(r"sort by", re.I)),
        page.locator('button[aria-haspopup="listbox"]'),
        page.locator('button[aria-haspopup="menu"]'),
        page.locator('button:has-text("Sort by")'),
        page.locator("button").filter(
            has_text=re.compile(
                r"featured|price: low to high|price: high to low|newest arrivals|low prices",
                re.I,
            )
        ),
    ]

    if click_first(sort_button_candidates, timeout=2200):
        page.wait_for_timeout(700)
        return True

    try:
        opened = bool(
            page.evaluate(
                """
                () => {
                    function cleanText(s) {
                        return (s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                    }

                    function isVisible(el) {
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
                    }

                    const els = Array.from(document.querySelectorAll("button, [role='button'], [aria-haspopup]"));
                    for (const el of els) {
                        const text = cleanText(el.innerText || el.textContent || el.getAttribute("aria-label") || "");
                        const popup = (el.getAttribute("aria-haspopup") || "").toLowerCase();
                        if (!isVisible(el)) continue;
                        if (
                            text.includes("sort by")
                            || text === "featured"
                            || text === "price: low to high"
                            || text === "price: high to low"
                            || text === "newest arrivals"
                            || text === "low prices"
                            || popup === "listbox"
                            || popup === "menu"
                        ) {
                            el.scrollIntoView({ block: "center", inline: "center" });
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }
                """
            )
        )
    except Exception:
        opened = False

    if opened:
        page.wait_for_timeout(700)
    return opened


def choose_sort_option(page, option_label: str) -> bool:
    escaped_label = re.escape(option_label)
    option_candidates = [
        page.get_by_role("option", name=re.compile(rf"^{escaped_label}$", re.I)),
        page.get_by_role("menuitemradio", name=re.compile(rf"^{escaped_label}$", re.I)),
        page.get_by_role("menuitem", name=re.compile(rf"^{escaped_label}$", re.I)),
        page.get_by_role("button", name=re.compile(rf"^{escaped_label}$", re.I)),
        page.get_by_role("link", name=re.compile(rf"^{escaped_label}$", re.I)),
        page.get_by_text(re.compile(rf"^{escaped_label}$", re.I)),
        page.locator(f'text="{option_label}"'),
    ]

    if click_first(option_candidates, timeout=2200):
        return True

    try:
        return bool(
            page.evaluate(
                """
                (targetLabel) => {
                    function cleanText(s) {
                        return (s || "").replace(/\\s+/g, " ").trim();
                    }

                    function isVisible(el) {
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
                    }

                    const wanted = cleanText(targetLabel).toLowerCase();
                    const els = Array.from(document.querySelectorAll("button, a, [role='option'], [role='menuitem'], [role='menuitemradio'], [role='button'], [aria-label]"));
                    for (const el of els) {
                        const text = cleanText(el.innerText || el.textContent || el.getAttribute("aria-label") || "").toLowerCase();
                        if (!isVisible(el)) continue;
                        if (text !== wanted) continue;
                        el.scrollIntoView({ block: "center", inline: "center" });
                        el.click();
                        return true;
                    }
                    return false;
                }
                """,
                option_label,
            )
        )
    except Exception:
        return False


def wait_for_sort_applied(page, sort_label: str, sort_rank: str, timeout_ms: int = 12000) -> bool:
    remaining = timeout_ms
    label_matcher = re.compile(re.escape(sort_label), re.I)

    while remaining > 0:
        current_url = page.url or ""
        selected_store = get_selected_store_text(page)

        if f"s={sort_rank}" in current_url:
            if wait_for_grid_to_appear(page, timeout_ms=2500):
                return True

        try:
            visible_sort_button = page.locator("button").filter(has_text=label_matcher).first
            if visible_sort_button.count() > 0 and visible_sort_button.is_visible(timeout=300):
                if wait_for_grid_to_appear(page, timeout_ms=2500):
                    return True
        except Exception:
            pass

        if selected_store and "columbus circle" not in normalize_text(selected_store):
            dismiss_popups(page)

        page.wait_for_timeout(400)
        remaining -= 400

    return False


def change_sort(page, sort_label: str, sort_rank: str) -> None:
    print(f"Changing sort to: {sort_label}")
    scroll_to_top(page)

    if open_sort_menu(page) and choose_sort_option(page, sort_label):
        page.wait_for_timeout(1800)
        dismiss_popups(page)
        if wait_for_sort_applied(page, sort_label, sort_rank, timeout_ms=10000):
            print(f"Applied sort via UI: {sort_label}")
            return

    print(f"UI sort change was unreliable for {sort_label}; falling back to a direct URL update.")
    current_url = page.url or SEARCH_DEALS_URL
    if "?" in current_url:
        if re.search(r"([?&])s=[^&]*", current_url):
            next_url = re.sub(r"([?&])s=[^&]*", rf"\\1s={sort_rank}", current_url, count=1)
        else:
            next_url = f"{current_url}&s={sort_rank}"
    else:
        next_url = f"{current_url}?s={sort_rank}"

    page.goto(next_url, wait_until="domcontentloaded")
    page.wait_for_timeout(2200)
    dismiss_popups(page)

    if not wait_for_sort_applied(page, sort_label, sort_rank, timeout_ms=12000):
        raise RuntimeError(f'Could not switch the search page to "{sort_label}".')

    print(f"Applied sort via URL fallback: {sort_label}")


def launch_browser(playwright):
    if GOOGLE_CHROME_EXECUTABLE.exists():
        try:
            browser = playwright.chromium.launch(
                executable_path=str(GOOGLE_CHROME_EXECUTABLE),
                headless=True,
                slow_mo=35,
            )
            print("Using installed Google Chrome in headless mode.")
            return browser
        except Exception as e:
            print(f"Installed Google Chrome launch failed, falling back to Chromium: {e}")

    print("Using Playwright Chromium in headless mode.")
    return playwright.chromium.launch(headless=True, slow_mo=35)


def wait_for_store_modal(page, timeout_ms: int = 8000):
    remaining = timeout_ms

    while remaining > 0:
        modal = get_store_modal(page)
        if modal is not None:
            return modal
        page.wait_for_timeout(250)
        remaining -= 250

    return None


def open_update_location_modal(page) -> None:
    dismiss_popups(page)
    wait_for_store_launcher(page, timeout_ms=8000)

    opened = click_first([
        page.get_by_role("button", name=re.compile(r"select a store", re.I)),
        page.get_by_role("button", name=re.compile(r"see store details", re.I)),
        page.get_by_role("button", name=re.compile(r"pickup at", re.I)),
        page.get_by_role("button", name=re.compile(r"delivery at", re.I)),
        page.get_by_role("link", name=re.compile(r"select a store", re.I)),
        page.locator('button[aria-label="Select a store"]'),
        page.locator('button[data-csa-c-content-id="Whole Foods Market"]'),
        page.locator('button[aria-label="See store details"]'),
        page.locator('button[data-csa-c-content-id^="My Store"]'),
        page.locator('button:has-text("Pickup at")'),
        page.locator('button:has-text("Delivery at")'),
        page.locator('button:has-text("Select a store")'),
        page.locator('button:has-text("Update location")'),
        page.locator('button:has-text("Find a store")'),
        page.get_by_role("button", name=re.compile(r"find a store", re.I)),
        page.get_by_role("link", name=re.compile(r"find a store", re.I)),
        page.get_by_text(re.compile(r"find a store", re.I)),
        page.get_by_role("button", name=re.compile(r"update location", re.I)),
        page.get_by_role("link", name=re.compile(r"update location", re.I)),
        page.get_by_text(re.compile(r"update location", re.I)),
        page.get_by_role("button", name=re.compile(r"change store", re.I)),
        page.get_by_role("link", name=re.compile(r"change store", re.I)),
        page.get_by_role("button", name=re.compile(r"my store", re.I)),
        page.get_by_role("link", name=re.compile(r"my store", re.I)),
    ], timeout=1800)

    if not opened:
        try:
            opened = bool(
                page.evaluate(
                    """
                    () => {
                        function cleanText(s) {
                            return (s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                        }

                        function isVisible(el) {
                            const rect = el.getBoundingClientRect();
                            const style = window.getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
                        }

                        const candidates = Array.from(document.querySelectorAll("button, a, [role='button'], [role='link'], [aria-label]"));
                        for (const el of candidates) {
                            const text = cleanText(el.innerText || el.textContent || el.getAttribute("aria-label") || "");
                            if (!isVisible(el)) continue;
                            if (!/(select a store|find a store|update location|change store|my store|whole foods market|see store details|pickup at|delivery at)/i.test(text)) continue;
                            el.scrollIntoView({ block: "center", inline: "center" });
                            el.click();
                            return true;
                        }
                        return false;
                    }
                    """
                )
            )
        except Exception:
            opened = False

    if not opened:
        print("Could not open the store modal from the deals page; opening the direct store modal URL.")
        page.goto(STORE_MODAL_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(1800)
        dismiss_popups(page)


def run_store_selection_cycle(page, cycle_number: int, close_after_selection: bool = False) -> None:
    if cycle_number > 1:
        print(f"Opening the location modal again for store selection pass {cycle_number}.")

    modal = wait_for_store_modal(page, timeout_ms=500)
    if modal is None:
        open_update_location_modal(page)
        page.wait_for_timeout(600)
        modal = wait_for_store_modal(page, timeout_ms=8000)
    selector_available = wait_for_store_iframe_text(
        page,
        r"Find a Whole Foods Market|Find a store near you|Locate a store|Columbus Circle",
        timeout_ms=1500,
    ) if modal is None else True
    if modal is None and not selector_available:
        raise RuntimeError(
            "Location modal did not appear, so the scraper refused to use the page-level search box."
        )

    columbus_visible = wait_for_store_iframe_text(page, r"Columbus Circle", timeout_ms=2500)
    if columbus_visible:
        print("Columbus Circle is already visible in the store selector; skipping store search input.")
    else:
        search_ok = fill_store_search_input_in_iframe(page, STORE_SEARCH_TEXT)
        if not search_ok:
            raise RuntimeError("Could not search for Columbus Circle inside the location modal.")

        if not wait_for_store_iframe_text(page, r"Shop Store|Make this my store|Columbus Circle", timeout_ms=12000):
            raise RuntimeError('The store search completed, but the Columbus Circle result did not appear in the store iframe.')

    made_store = click_make_this_my_store_for_columbus(page)
    if not made_store:
        raise RuntimeError('Could not click the Columbus Circle store CTA.')

    print(f"Clicked the Columbus Circle store CTA on pass {cycle_number}; waiting for the modal to auto-close.")
    page.wait_for_timeout(POST_STORE_SET_WAIT_MS)
    dismiss_popups(page)

    if close_after_selection:
        page.wait_for_timeout(1200)
        dismiss_popups(page)
        modal_after_close = wait_for_store_modal(page, timeout_ms=3000)
        if modal_after_close is not None:
            print(f"Pass {cycle_number} left a modal layer open; clicking the center of the screen.")
            if not click_center_of_viewport(page):
                raise RuntimeError('The second store-selection pass left a modal open, but the center-screen click could not be sent.')
            page.wait_for_timeout(3000)
            dismiss_popups(page)
            wait_for_store_modal_to_disappear(page, timeout_ms=6000)
        else:
            page.wait_for_timeout(3000)
            dismiss_popups(page)
    else:
        if not wait_for_store_modal_to_disappear(page, timeout_ms=9000):
            print(f'Pass {cycle_number} did not auto-close; clicking the center of the screen as a fallback.')
            if not click_center_of_viewport(page):
                raise RuntimeError("Clicked the Columbus Circle store CTA, but the location modal never auto-closed.")
            page.wait_for_timeout(3000)
            dismiss_popups(page)
            if not wait_for_store_modal_to_disappear(page, timeout_ms=6000):
                raise RuntimeError("Clicked the Columbus Circle store CTA, but the location modal never auto-closed.")

        page.wait_for_timeout(1200)
        dismiss_popups(page)

    selected_store = get_selected_store_text(page)
    print(f"Selected store text after pass {cycle_number}: {selected_store or '<empty>'}")


def set_store_from_search_page(page) -> None:
    print(f"Opening sales search page for store setup: {SEARCH_DEALS_URL}")
    page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
    dismiss_popups(page)
    if not wait_for_store_launcher(page, timeout_ms=8000):
        print('Store launcher was not detected quickly; continuing and attempting the modal flow anyway.')

    last_error = None

    for attempt in range(2):
        if attempt > 0:
            print("Retrying store modal flow after the page still did not show Columbus Circle.")
            dismiss_popups(page)

        try:
            run_store_selection_cycle(page, cycle_number=1)
            used_direct_store_locator = "/stores" in page.url
            if used_direct_store_locator:
                page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            dismiss_popups(page)

            selected_store = get_selected_store_text(page)
            print(f"Selected store text after first-pass verification: {selected_store or '<empty>'}")

            if wait_for_selected_store_text(page, r"columbus\s+circle", timeout_ms=5000):
                print("Confirmed selected store after the first pass: Columbus Circle.")
                return

            print("First store-selection pass did not set Columbus Circle; opening the location modal again for store selection pass 2.")
            run_store_selection_cycle(page, cycle_number=2, close_after_selection=True)
            used_direct_store_locator = "/stores" in page.url
            if used_direct_store_locator:
                page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
        except Exception as exc:
            last_error = exc
            continue

        print("Second store-selection pass finished.")
        page.wait_for_timeout(2500)
        dismiss_popups(page)

        selected_store = get_selected_store_text(page)
        print(f"Selected store text after both passes: {selected_store or '<empty>'}")

        if wait_for_selected_store_text(page, r"columbus\s+circle", timeout_ms=8000):
            print("Confirmed selected store: Columbus Circle.")
            return

        last_error = RuntimeError(
            'The page still did not show "Columbus Circle" as the selected store after two full store-selection passes.'
        )

    if last_error is not None:
        raise last_error

    raise RuntimeError("Store selection failed before scraping could begin.")


def crawl_current_sort(page, products_by_asin: dict, sort_label: str) -> int:
    scroll_to_top(page)
    page.wait_for_timeout(1500)
    dismiss_popups(page)
    wait_for_grid_to_appear(page, timeout_ms=10000)

    added = merge_products_from_current_page(page, products_by_asin)
    print(f"{sort_label}: initial products captured {len(products_by_asin)} (+{added})")

    stable_rounds = 0
    stale_load_more_rounds = 0
    last_height = -1
    total_added_for_sort = added

    for round_index in range(MAX_ACTION_ROUNDS):
        before_count = len(products_by_asin)
        before_height = page.evaluate("() => document.documentElement.scrollHeight")
        before_tile_count = current_rendered_product_count(page)

        did_scroll = scroll_page_down(page)
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        dismiss_popups(page)
        added_after_scroll = merge_products_from_current_page(page, products_by_asin)

        clicked_load_more = click_load_more(page)
        added_after_click = 0

        if clicked_load_more:
            wait_for_results_growth(page, before_height, before_tile_count, timeout_ms=9000)
            page.wait_for_timeout(LOAD_MORE_SETTLE_MS)
            dismiss_popups(page)
            wait_for_grid_to_appear(page, timeout_ms=8000)
            added_after_click = merge_products_from_current_page(page, products_by_asin)

        after_count = len(products_by_asin)
        after_height = page.evaluate("() => document.documentElement.scrollHeight")
        added_this_round = after_count - before_count
        total_added_for_sort += added_this_round

        print(
            f"{sort_label} round {round_index + 1}: total={after_count} "
            f"(+{added_this_round}), scroll={'y' if did_scroll else 'n'}, "
            f"load_more={'y' if clicked_load_more else 'n'}"
        )

        content_changed = (
            added_after_scroll > 0
            or added_after_click > 0
            or after_height > before_height
        )

        load_more_was_stale = (
            clicked_load_more
            and added_after_click == 0
            and after_count == before_count
            and after_height <= before_height
        )

        if load_more_was_stale:
            stale_load_more_rounds += 1
        else:
            stale_load_more_rounds = 0

        if not content_changed and after_height == last_height:
            stable_rounds += 1
        else:
            stable_rounds = 0

        last_height = after_height

        if stale_load_more_rounds >= 2:
            print(f"{sort_label}: stopping because Load More did not add any new products in consecutive rounds.")
            break

        if stable_rounds >= 4:
            print(f"{sort_label}: stopping because the page stopped growing and no new products were detected.")
            break

    page.wait_for_timeout(FINAL_SETTLE_MS)
    final_added = merge_products_from_current_page(page, products_by_asin)
    total_added_for_sort += final_added
    print(f"{sort_label}: final settle captured +{final_added}; total unique products now {len(products_by_asin)}")
    return total_added_for_sort


def discover_search_deals() -> dict:
    products_by_asin = {}
    captured_batch_urls = []
    sort_runs_summary = []
    search_mode = os.environ.get("WHOLEFOODS_SEARCH_MODE", "full").strip().lower()
    if search_mode not in {"fast", "full"}:
        print(f'Unknown WHOLEFOODS_SEARCH_MODE="{search_mode}"; falling back to full.')
        search_mode = "full"

    run_plans = [
        {
            "filter_label": None,
            "rh_values": [],
            "sorts": FAST_BASE_SORT_RUNS if search_mode == "fast" else BASE_SORT_RUNS,
        },
    ]
    if search_mode == "full":
        run_plans.extend(FILTER_RUNS)
    else:
        print("Running Whole Foods search in fast mode: base high-yield sorts only, optional filters skipped.")

    total_runs = sum(len(plan["sorts"]) for plan in run_plans)
    completed_runs = 0
    started_at = time.monotonic()

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1100},
            locale="en-US",
            timezone_id="America/New_York",
            user_agent=HEADLESS_CHROME_USER_AGENT,
        )
        context.add_init_script(STEALTH_INIT_SCRIPT)
        page = context.new_page()
        set_store_from_search_page(page)

        if not wait_for_selected_store_text(page, r"columbus circle", timeout_ms=6000):
            print("Pre-scroll store check failed; retrying the store modal flow.")
            set_store_from_search_page(page)
            if not wait_for_selected_store_text(page, r"columbus circle", timeout_ms=6000):
                raise RuntimeError(
                    'The page still did not show "Columbus Circle" as the selected store before deals scrolling began.'
                )

        def handle_response(response):
            if "getGridAsins" in response.url:
                captured_batch_urls.append(response.url)

        page.on("response", handle_response)

        page.wait_for_timeout(1800)
        dismiss_popups(page)
        wait_for_grid_to_appear(page, timeout_ms=10000)

        for plan_index, plan in enumerate(run_plans):
            filter_label = plan["filter_label"]
            rh_values = plan["rh_values"]
            sorts = plan["sorts"]

            if plan_index == 0:
                print("\nRunning base search deals crawl across all requested sorts.")
            else:
                print(f"\nRunning filtered crawl for: {filter_label}")

            for sort_label, sort_rank in sorts:
                run_label = sort_label if not filter_label else f"{filter_label} / {sort_label}"
                print_search_progress(
                    completed_runs,
                    total_runs,
                    started_at,
                    len(products_by_asin),
                    f"starting {run_label}",
                )

                try:
                    open_search_run(page, run_label, sort_label, sort_rank, extra_rh_values=rh_values)
                except (RuntimeError, PlaywrightError) as exc:
                    if not filter_label:
                        raise

                    print(f"{run_label}: skipping optional filtered run after page/render failure: {exc}")
                    sort_runs_summary.append(
                        {
                            "filter_label": filter_label,
                            "rh_values": rh_values,
                            "sort_label": sort_label,
                            "sort_rank": sort_rank,
                            "run_label": run_label,
                            "new_products_found": 0,
                            "captured_events": 0,
                            "total_products_after_sort": len(products_by_asin),
                            "skipped": True,
                            "error": str(exc),
                        }
                    )
                    write_search_partial_checkpoint(products_by_asin, captured_batch_urls, sort_runs_summary)
                    completed_runs += 1
                    print_search_progress(
                        completed_runs,
                        total_runs,
                        started_at,
                        len(products_by_asin),
                        f"skipped {run_label}",
                    )
                    continue

                before_sort_count = len(products_by_asin)
                added_for_sort = crawl_current_sort(page, products_by_asin, run_label)
                new_products_found = len(products_by_asin) - before_sort_count
                sort_runs_summary.append(
                    {
                        "filter_label": filter_label,
                        "rh_values": rh_values,
                        "sort_label": sort_label,
                        "sort_rank": sort_rank,
                        "run_label": run_label,
                        "new_products_found": new_products_found,
                        "captured_events": added_for_sort,
                        "total_products_after_sort": len(products_by_asin),
                        "skipped": False,
                    }
                )
                write_search_partial_checkpoint(products_by_asin, captured_batch_urls, sort_runs_summary)
                completed_runs += 1
                print_search_progress(
                    completed_runs,
                    total_runs,
                    started_at,
                    len(products_by_asin),
                    f"finished {run_label}; +{new_products_found} new",
                )

        browser.close()

    ordered_products = [products_by_asin[k] for k in sorted(products_by_asin)]

    return {
        "search_url": SEARCH_DEALS_URL,
        "product_count": len(ordered_products),
        "network_batch_count": len(captured_batch_urls),
        "sort_runs": sort_runs_summary,
        "products": ordered_products,
    }


if __name__ == "__main__":
    result = discover_search_deals()

    with open("search_deals_products.json", "w", encoding="utf-8") as f:
        json.dump(result["products"], f, indent=2, ensure_ascii=False)

    with open("search_deals_report.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "search_url": result["search_url"],
                "product_count": result["product_count"],
                "network_batch_count": result["network_batch_count"],
                "sort_runs": result["sort_runs"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"\nDone. Unique products collected: {result['product_count']}")
