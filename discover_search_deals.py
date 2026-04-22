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
    click_first,
    dismiss_popups,
    fill_store_search_input,
    get_store_modal,
    parse_all_deals_html,
)


BASE_DEALS_RH = "p_n_deal_type:23566065011"
SEARCH_RESULTS_CATEGORY = "18473610011"
SEARCH_RESULTS_PAGE_SIZE = 30
NETWORK_STALE_BATCH_LIMIT = 2
NETWORK_HTTP_RETRY_LIMIT = 2
NETWORK_TAIL_BATCH_SIZES = [20, 15, 10]
NETWORK_RESULT_WINDOW_LIMIT = 500
SEARCH_DEALS_BASE_URL = "https://www.wholefoodsmarket.com/grocery/search"
WHOLE_FOODS_HOME_URL = "https://www.wholefoodsmarket.com/"
STORE_MODAL_URL = "https://www.wholefoodsmarket.com/stores?modalView=true"
SEARCH_DEALS_URL = (
    f"{SEARCH_DEALS_BASE_URL}?{urlencode({'k': '', 'rh': BASE_DEALS_RH, 's': 'relevanceblender'})}"
)
SEARCH_DEALS_PARTIAL_PRODUCTS_FILE = Path(__file__).with_name("search_deals_products.partial.json")
SEARCH_DEALS_PARTIAL_REPORT_FILE = Path(__file__).with_name("search_deals_report.partial.json")
SEARCH_FAILURE_DEBUG_DIR = Path(__file__).with_name("logs")
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

KNOWN_STORE_SEARCH_HINTS = {
    "10160": ["10019", "Columbus Circle", "10 Columbus Cir"],
    "10328": ["10025", "Upper West Side", "808 Columbus Ave"],
}


def store_search_text(store: Optional[dict]) -> str:
    return (store or {}).get("name") or "Columbus Circle"


def store_display_name(store: Optional[dict]) -> str:
    return (store or {}).get("name") or store_search_text(store)


def store_selected_pattern(store: Optional[dict]) -> str:
    return re.escape(store_display_name(store))


def store_search_terms(store: Optional[dict]) -> list[str]:
    store = store or {}
    target_store_id = str(store.get("id") or "").strip()
    terms = []

    terms.extend(KNOWN_STORE_SEARCH_HINTS.get(target_store_id, []))

    for key in ("search_text", "postal_code", "zip", "address", "label", "name"):
        value = (store.get(key) or "").strip()
        if value:
            terms.append(value)

    seen = set()
    ordered = []
    for term in terms:
        normalized = normalize_text(term)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(term)

    return ordered or ["Columbus Circle"]


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

    input_box = None
    for selector in (
        "#store-finder-search-bar",
        "#postalCode",
        'input[name="postalCode"]',
        'input[placeholder*="postal code" i]',
        'input[placeholder*="city or state" i]',
        'input[type="text"]',
    ):
        candidate = iframe.locator(selector).first
        try:
            candidate.wait_for(state="visible", timeout=2000)
            input_box = candidate
            break
        except Exception:
            continue

    if input_box is None:
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


def click_make_this_my_store_for_store(page, store: Optional[dict]) -> bool:
    iframe = wait_for_store_iframe(page, timeout_ms=5000)
    scope = iframe or page
    target_store_id = str((store or {}).get("id") or "").strip()
    target_name = store_display_name(store)
    target_terms = [normalize_text(term) for term in store_search_terms(store) if normalize_text(term)]

    def target_card_text_matches(text: str) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        if len(normalized) > 450:
            return False

        if normalize_text(target_name) not in normalized:
            return False

        alternate_terms = [term for term in target_terms if term != normalize_text(target_name)]
        if not alternate_terms:
            return True
        return any(term in normalized for term in alternate_terms)

    def click_card_action(card) -> bool:
        try:
            card_text = card.inner_text(timeout=1200)
        except Exception:
            card_text = ""

        if not target_card_text_matches(card_text):
            return False

        try:
            return bool(
                card.evaluate(
                    """
                    (root) => {
                        const controls = Array.from(root.querySelectorAll('a, button, input, [role="button"], span[tabindex]'));
                        for (const candidate of controls) {
                            const text = `${candidate.innerText || ''} ${candidate.textContent || ''} ${candidate.value || ''}`.toLowerCase();
                            if (!/(shop store|make this my store|select store|choose store|set as my store)/i.test(text)) continue;
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
        except Exception:
            return False

    def click_target_action_in_scope(scope) -> bool:
        try:
            return bool(
                scope.evaluate(
                    """
                    ({ targetName, targetTerms }) => {
                        function cleanText(value) {
                            return (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
                        }

                        function isVisible(el) {
                            const rect = el.getBoundingClientRect();
                            const style = window.getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
                        }

                        function matchesStore(text) {
                            const normalized = cleanText(text);
                            if (!normalized || !normalized.includes(cleanText(targetName))) return false;
                            const alternates = (targetTerms || []).filter((term) => term && term !== cleanText(targetName));
                            if (!alternates.length) return true;
                            return alternates.some((term) => normalized.includes(cleanText(term)));
                        }

                        const shopLabels = Array.from(document.querySelectorAll("span, a, button, div")).filter((node) => {
                            return cleanText(node.innerText || node.textContent || "") === "shop store";
                        });

                        for (const label of shopLabels) {
                            let row = label;
                            for (let depth = 0; row && depth < 14; depth += 1, row = row.parentElement) {
                                const rowText = cleanText(row.innerText || row.textContent || "");
                                if (!matchesStore(rowText)) continue;

                                const clickTarget =
                                    label.closest(".a-declarative")
                                    || label.closest(".list-selection-pickup")
                                    || label.closest(".a-button")
                                    || label.closest("button")
                                    || label.closest("a")
                                    || label;

                                if (!clickTarget || !isVisible(clickTarget)) continue;

                                clickTarget.scrollIntoView({ block: "center", inline: "center" });
                                ["mousedown", "mouseup", "click"].forEach((type) => {
                                    clickTarget.dispatchEvent(
                                        new MouseEvent(type, { bubbles: true, cancelable: true, view: window })
                                    );
                                });
                                if (typeof clickTarget.click === "function") clickTarget.click();
                                return true;
                            }
                        }

                        const actionableContainers = Array.from(document.querySelectorAll("li, article, section, div"));
                        const ranked = [];
                        for (const container of actionableContainers) {
                            if (!isVisible(container)) continue;
                            const text = cleanText(container.innerText || container.textContent || "");
                            if (!matchesStore(text)) continue;

                            const controls = Array.from(
                                container.querySelectorAll('a, button, input, [role="button"], span, div')
                            );
                            const targetControl = controls.find((candidate) => {
                                const controlText = cleanText(
                                    `${candidate.innerText || ""} ${candidate.textContent || ""} ${candidate.value || ""} ${candidate.getAttribute?.("aria-label") || ""}`
                                );
                                return /(shop store|make this my store|select store|choose store|set as my store)/i.test(controlText);
                            });
                            if (!targetControl) continue;

                            ranked.push({ container, targetControl, textLength: text.length });
                        }

                        ranked.sort((a, b) => a.textLength - b.textLength);
                        const best = ranked[0];
                        if (!best || !isVisible(best.targetControl)) return false;

                        best.targetControl.scrollIntoView({ block: "center", inline: "center" });
                        ["mousedown", "mouseup", "click"].forEach((type) => {
                            best.targetControl.dispatchEvent(
                                new MouseEvent(type, { bubbles: true, cancelable: true, view: window })
                            );
                        });
                        if (typeof best.targetControl.click === "function") best.targetControl.click();
                        return true;
                    }
                    """,
                    {"targetName": target_name, "targetTerms": target_terms},
                )
            )
        except Exception:
            return False

    if click_target_action_in_scope(scope):
        return True

    try:
        shop_labels = scope.get_by_text("Shop Store", exact=True)
        label_count = shop_labels.count()
    except Exception:
        label_count = 0

    for index in range(min(label_count, 60)):
        label = shop_labels.nth(index)
        try:
            ancestor_texts = label.evaluate(
                """
                (el) => {
                    const rows = [];
                    let cur = el;
                    for (let depth = 0; cur && depth < 14; depth += 1, cur = cur.parentElement) {
                        rows.push((cur.innerText || cur.textContent || "").replace(/\\s+/g, " ").trim());
                    }
                    return rows;
                }
                """
            )
        except Exception:
            continue

        if not any(target_card_text_matches(text) for text in ancestor_texts or []):
            continue

        try:
            clicked = bool(
                label.evaluate(
                    """
                    (el) => {
                        const clickTarget =
                            el.closest(".a-declarative")
                            || el.closest(".list-selection-pickup")
                            || el.closest(".a-button")
                            || el.closest("button")
                            || el.closest("a")
                            || el;
                        clickTarget.scrollIntoView({ block: "center", inline: "center" });
                        ["mousedown", "mouseup", "click"].forEach((type) => {
                            clickTarget.dispatchEvent(
                                new MouseEvent(type, { bubbles: true, cancelable: true, view: window })
                            );
                        });
                        if (typeof clickTarget.click === "function") clickTarget.click();
                        return true;
                    }
                    """
                )
            )
        except Exception:
            clicked = False

        if clicked:
            return True

    if target_store_id:
        target_card = scope.locator(f'li[data-bu="{target_store_id}"], article[data-bu="{target_store_id}"]').first
        if click_card_action(target_card):
            return True
        targeted_locators = [
            target_card.get_by_text(STORE_SELECT_CTA_PATTERN),
            target_card.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
            target_card.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
            target_card.locator('button:has-text("Shop Store")'),
            target_card.locator('a:has-text("Shop Store")'),
            target_card.locator('button'),
            target_card.locator('a'),
        ]
        if click_first_no_wait(targeted_locators, timeout=2400):
            return True

    name_card = scope.locator("li, article, [data-testid], [class*='store']").filter(
        has_text=re.compile(re.escape(target_name), re.I)
    ).first
    if click_card_action(name_card):
        return True
    targeted_locators = [
        name_card.get_by_text(STORE_SELECT_CTA_PATTERN),
        name_card.get_by_role("button", name=STORE_SELECT_CTA_PATTERN),
        name_card.get_by_role("link", name=STORE_SELECT_CTA_PATTERN),
        name_card.locator('button:has-text("Shop Store")'),
        name_card.locator('a:has-text("Shop Store")'),
        name_card.locator('button'),
        name_card.locator('a'),
    ]
    if click_first_no_wait(targeted_locators, timeout=2400):
        return True

    return False


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


def get_page_store_context(page) -> dict:
    try:
        payload = parse_next_data_payload(page.content()) or {}
    except Exception:
        return {}

    page_props = payload.get("props", {}).get("pageProps", {})
    wfm_location = page_props.get("wfmccLocationData", {}) or {}
    catering_context = wfm_location.get("cateringStoreContext", {}) or {}
    alm_attributes = catering_context.get("almAttributes", {}) or {}
    store_preference = catering_context.get("storePreference", {}) or {}
    location_info = store_preference.get("locationInfo", {}) or {}

    return {
        "store_id": str(alm_attributes.get("storeId") or store_preference.get("buid") or "").strip(),
        "store_name": (store_preference.get("storeName") or "").strip(),
        "postal_code": (location_info.get("postalCode") or "").strip(),
        "street_address": (location_info.get("streetAddress") or "").strip(),
    }


def wait_for_selected_store(page, store: Optional[dict], timeout_ms: int = 8000) -> bool:
    remaining = timeout_ms
    target_pattern = re.compile(store_selected_pattern(store), re.I)
    target_store_id = str((store or {}).get("id") or "").strip()
    target_terms = [normalize_text(term) for term in store_search_terms(store) if normalize_text(term)]

    while remaining > 0:
        selected_store = get_selected_store_text(page)
        if selected_store and (
            target_pattern.search(selected_store)
            or target_pattern.search(normalize_text(selected_store))
        ):
            return True

        page_context = get_page_store_context(page)
        page_store_id = page_context.get("store_id") or ""
        page_store_name = normalize_text(page_context.get("store_name") or "")
        page_postal_code = normalize_text(page_context.get("postal_code") or "")
        page_street_address = normalize_text(page_context.get("street_address") or "")

        if target_store_id and page_store_id == target_store_id:
            return True

        if page_store_name and normalize_text(store_display_name(store)) == page_store_name:
            return True

        if target_terms and any(
            term and (
                term == page_postal_code
                or term in page_store_name
                or term in page_street_address
            )
            for term in target_terms
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


def page_has_search_results(page) -> bool:
    try:
        if current_rendered_product_count(page) > 0:
            return True
    except Exception:
        pass

    try:
        html = page.content()
    except Exception:
        return False

    return bool(parse_next_data_products(html))


def wait_for_search_results(page, timeout_ms: int = 30000) -> bool:
    remaining = timeout_ms

    while remaining > 0:
        try:
            if current_rendered_product_count(page) > 0:
                return True
        except Exception:
            pass

        try:
            if page_has_search_results(page):
                return True
        except Exception:
            pass

        page.wait_for_timeout(500)
        remaining -= 500

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


def build_rsi_search_url(
    offer_listing_discriminator: str,
    sort_rank: str,
    offset: int,
    size: int = SEARCH_RESULTS_PAGE_SIZE,
    extra_rh_values: Optional[list[str]] = None,
) -> str:
    filters = [BASE_DEALS_RH]
    if extra_rh_values:
        filters.extend(extra_rh_values)

    return (
        "https://www.wholefoodsmarket.com/api/wwos/rsi/search?"
        + urlencode(
            {
                "text": "",
                "old": offer_listing_discriminator,
                "offset": offset,
                "size": size,
                "sort": sort_rank,
                "programType": "GROCERY",
                "filters": ",".join(filters),
                "categories": SEARCH_RESULTS_CATEGORY,
            }
        )
    )


def build_products_url(offer_listing_discriminator: str, asins: list[str]) -> str:
    return (
        "https://www.wholefoodsmarket.com/api/wwos/products?"
        + urlencode(
            {
                "offerListingDiscriminator": offer_listing_discriminator,
                "programType": "GROCERY",
                "asins": ",".join(asins),
            }
        )
    )


def goto_search_url(page, target_url: str, run_label: str, attempts: int = 2) -> None:
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            response = page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            status = response.status if response else None
            if status and status >= 400:
                raise RuntimeError(f"search page returned HTTP {status}")
            return
        except PlaywrightError as exc:
            last_error = exc
        except RuntimeError as exc:
            last_error = exc

        if attempt >= attempts:
            break

        print(f"{run_label}: navigation failed on attempt {attempt}; warming the Whole Foods home page before retry.")
        try:
            home_response = page.goto(WHOLE_FOODS_HOME_URL, wait_until="domcontentloaded", timeout=60000)
            home_status = home_response.status if home_response else None
            print(f"{run_label}: home warm status={home_status}")
            page.wait_for_timeout(2500)
            dismiss_popups(page)
        except Exception:
            pass

        page.wait_for_timeout(3000)

    raise RuntimeError(f'Could not open products for the run "{run_label}": {last_error}')


def normalized_url(url: str) -> str:
    return (url or "").rstrip("/")


def current_page_matches_run(page, target_url: str, target_pattern: str) -> bool:
    current_url = normalized_url(page.url or "")
    wanted_url = normalized_url(target_url)
    if current_url != wanted_url:
        return False
    return bool(target_pattern) and wait_for_selected_store_text(page, target_pattern, timeout_ms=1500)


def write_search_partial_checkpoint(products_by_asin: dict, network_batch_count: int, sort_runs_summary: list) -> None:
    ordered_products = [products_by_asin[k] for k in sorted(products_by_asin)]

    with open(SEARCH_DEALS_PARTIAL_PRODUCTS_FILE, "w", encoding="utf-8") as f:
        json.dump(ordered_products, f, indent=2, ensure_ascii=False)

    with open(SEARCH_DEALS_PARTIAL_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "search_url": SEARCH_DEALS_URL,
                "product_count": len(ordered_products),
                "network_batch_count": network_batch_count,
                "sort_runs": sort_runs_summary,
                "partial": True,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )


def write_search_failure_debug(page, run_label: str, store: Optional[dict], reason: str) -> None:
    try:
        SEARCH_FAILURE_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_label = re.sub(r"[^a-z0-9]+", "_", (run_label or "search").lower()).strip("_") or "search"
        safe_store = re.sub(r"[^a-z0-9]+", "_", store_display_name(store).lower()).strip("_") or "store"

        html_path = SEARCH_FAILURE_DEBUG_DIR / f"search_failure_{safe_store}_{safe_label}.html"
        json_path = SEARCH_FAILURE_DEBUG_DIR / f"search_failure_{safe_store}_{safe_label}.json"

        html = page.content()
        html_path.write_text(html, encoding="utf-8")

        body_text = ""
        try:
            body_text = page.locator("body").inner_text(timeout=2500)
        except Exception:
            pass

        next_products = parse_next_data_products(html)
        page_type = None
        next_data_found = False

        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
            html,
            flags=re.S,
        )
        if match:
            next_data_found = True
            try:
                data = json.loads(match.group(1))
                page_type = (
                    data.get("props", {})
                    .get("pageProps", {})
                    .get("pageType")
                )
            except Exception:
                page_type = "parse_error"

        payload = {
            "run_label": run_label,
            "store_name": store_display_name(store),
            "store_id": str((store or {}).get("id") or ""),
            "reason": reason,
            "url": page.url,
            "current_rendered_product_count": current_rendered_product_count(page),
            "next_data_found": next_data_found,
            "next_data_page_type": page_type,
            "next_data_product_count": len(next_products),
            "body_sample": body_text[:4000],
            "html_path": str(html_path),
        }
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"{run_label}: wrote failure debug to {json_path}")
    except Exception as exc:
        print(f"{run_label}: failed to write search failure debug: {exc}")


def open_search_run(page, run_label: str, sort_label: str, sort_rank: str, extra_rh_values: Optional[list[str]] = None, store: Optional[dict] = None) -> None:
    target_url = build_search_url(sort_rank, extra_rh_values=extra_rh_values)
    target_store = store_display_name(store)
    target_pattern = store_selected_pattern(store)
    print(f"Opening run: {run_label} ({sort_label})")
    print(f"Run URL: {target_url}")

    last_render_error = None

    for render_attempt in range(1, 4):
        if render_attempt > 1:
            print(f'{run_label}: products did not render on attempt {render_attempt - 1}; retrying the run URL.')
            page.wait_for_timeout(3000)

        reused_current_page = False
        if render_attempt == 1 and current_page_matches_run(page, target_url, target_pattern):
            reused_current_page = True
            print(f"{run_label}: reusing the already-loaded search page after store selection.")
            page.wait_for_timeout(2500)
            dismiss_popups(page)
        else:
            goto_search_url(page, target_url, run_label, attempts=2)
            page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
            dismiss_popups(page)

        if not wait_for_selected_store(page, store, timeout_ms=5000):
            print(f"{run_label}: store check failed after navigation; retrying the store modal flow.")
            set_store_from_search_page(page, store)
            if current_page_matches_run(page, target_url, target_pattern):
                page.wait_for_timeout(2500)
                dismiss_popups(page)
            else:
                goto_search_url(page, target_url, run_label, attempts=2)
                page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
                dismiss_popups(page)

            if not wait_for_selected_store(page, store, timeout_ms=6000):
                last_render_error = (
                    f'The page did not keep "{target_store}" selected while opening the run "{run_label}".'
                )
                continue

        if wait_for_search_results(page, timeout_ms=35000 if reused_current_page else 28000):
            if current_rendered_product_count(page) <= 0 and page_has_search_results(page):
                print(f"{run_label}: accepted Next.js search payload even though the visual grid mounted late.")
            return

        last_render_error = f'Products did not render for the run "{run_label}".'
        try:
            debug_body(page)
        except Exception:
            pass

    write_search_failure_debug(page, run_label, store, last_render_error or "render failure")

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


def parse_next_data_payload(html: str) -> Optional[dict]:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
        html,
        flags=re.S,
    )
    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def extract_offer_listing_discriminator(html: str) -> Optional[str]:
    data = parse_next_data_payload(html) or {}
    page_props = data.get("props", {}).get("pageProps", {})
    return (
        page_props.get("wfmccLocationData", {})
        .get("cateringStoreContext", {})
        .get("almAttributes", {})
        .get("offerListingDiscriminator")
    )


def normalize_products_api_item(item: dict) -> Optional[dict]:
    asin = item.get("asin")
    if not asin:
        return None

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
        first_image = images[0]
        if isinstance(first_image, dict):
            image = (
                first_image.get("url")
                or first_image.get("src")
                or first_image.get("small")
                or first_image.get("medium")
                or first_image.get("large")
            )
        else:
            image = first_image

    url = f"https://www.wholefoodsmarket.com/grocery/product/{asin}"

    return {
        "asin": asin,
        "brand": item.get("brandName"),
        "name": item.get("name"),
        "image": image,
        "url": url,
        "current_price": format_money(price.get("priceAmount")),
        "basis_price": format_money(price.get("basisPriceAmount")),
        "prime_price": format_money(prime_benefit.get("priceAmount")),
        "discount": discount,
        "unit_price": format_unit_price(offer_details.get("unitPrice")),
        "availability": item.get("availability"),
    }


def fetch_text_via_page(page, url: str, timeout_ms: int = 30000) -> tuple[int, str]:
    result = page.evaluate(
        """
        async ({ url, timeoutMs }) => {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeoutMs);
            try {
                const response = await fetch(url, {
                    credentials: "include",
                    signal: controller.signal,
                    headers: {
                        "accept": "application/json, text/plain, */*"
                    }
                });
                const text = await response.text();
                return { status: response.status, text };
            } catch (error) {
                return { status: 0, text: String(error) };
            } finally {
                clearTimeout(timer);
            }
        }
        """,
        {"url": url, "timeoutMs": timeout_ms},
    )
    return int(result.get("status") or 0), result.get("text") or ""


def parse_positive_int_env(name: str) -> Optional[int]:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        print(f'Ignoring invalid {name}="{raw}" (expected integer).')
        return None
    if value <= 0:
        print(f'Ignoring invalid {name}="{raw}" (must be > 0).')
        return None
    return value


def parse_rsi_search_asins(payload: dict) -> list[str]:
    results = (
        payload.get("mainResultSet", {})
        .get("searchResults", [])
    )
    asins = []
    for item in results:
        asin = item.get("asin")
        if asin:
            asins.append(asin)
    return asins


def parse_rsi_total_count(payload: dict) -> Optional[int]:
    main_result_set = payload.get("mainResultSet", {}) or {}

    for key in (
        "availableTotalResultCount",
        "totalResultCount",
        "approximateTotalResultCount",
        "totalResultCountPreVE",
        "searchResultsCount",
        "numberOfResults",
    ):
        value = main_result_set.get(key)
        if isinstance(value, int) and value > 0:
            return value
        if isinstance(value, str):
            try:
                parsed = int(value)
            except ValueError:
                continue
            if parsed > 0:
                return parsed

    return None


def fetch_rsi_payload_with_tail_fallback(
    page,
    offer_listing_discriminator: str,
    sort_label: str,
    sort_rank: str,
    offset: int,
    extra_rh_values: Optional[list[str]] = None,
) -> tuple[dict, list[str], Optional[int], int, int]:
    batch_sizes = [SEARCH_RESULTS_PAGE_SIZE]
    if offset > 0:
        batch_sizes.extend(size for size in NETWORK_TAIL_BATCH_SIZES if size < SEARCH_RESULTS_PAGE_SIZE)

    last_status = 0
    last_error = None

    for size_index, batch_size in enumerate(batch_sizes):
        status = 0
        text = ""
        rsi_url = build_rsi_search_url(
            offer_listing_discriminator,
            sort_rank,
            offset=offset,
            size=batch_size,
            extra_rh_values=extra_rh_values,
        )

        for attempt in range(1, NETWORK_HTTP_RETRY_LIMIT + 1):
            status, text = fetch_text_via_page(page, rsi_url)
            if status == 200:
                break
            if attempt < NETWORK_HTTP_RETRY_LIMIT:
                print(
                    f"{sort_label}: rsi/search returned HTTP {status} at offset {offset} "
                    f"(size={batch_size}); retrying ({attempt}/{NETWORK_HTTP_RETRY_LIMIT - 1})."
                )
                page.wait_for_timeout(1200)

        last_status = status

        if status != 200:
            if size_index < len(batch_sizes) - 1:
                print(
                    f"{sort_label}: rsi/search HTTP {status} at offset {offset} with size={batch_size}; "
                    "trying a smaller tail batch."
                )
                page.wait_for_timeout(700)
                continue
            return {}, [], None, 0, last_status

        try:
            payload = json.loads(text)
        except Exception as exc:
            last_error = exc
            if size_index < len(batch_sizes) - 1:
                print(
                    f"{sort_label}: could not parse rsi/search JSON at offset {offset} "
                    f"(size={batch_size}); trying a smaller tail batch."
                )
                page.wait_for_timeout(700)
                continue
            raise RuntimeError(f'{sort_label}: could not parse rsi/search JSON at offset {offset}: {exc}')

        asins = parse_rsi_search_asins(payload)
        expected_total = parse_rsi_total_count(payload)
        if asins or batch_size == SEARCH_RESULTS_PAGE_SIZE or size_index == len(batch_sizes) - 1:
            return payload, asins, expected_total, batch_size, 200

        print(
            f"{sort_label}: empty rsi/search batch at offset {offset} with size={batch_size}; "
            "trying a smaller tail batch."
        )
        page.wait_for_timeout(700)

    if last_error is not None:
        raise RuntimeError(f'{sort_label}: could not parse rsi/search JSON at offset {offset}: {last_error}')
    return {}, [], None, 0, last_status


def parse_next_data_products(html: str) -> list[dict]:
    data = parse_next_data_payload(html)
    if not data:
        return []

    page_props = data.get("props", {}).get("pageProps", {})
    products_info = (
        page_props.get("productsInfo")
        or page_props.get("searchResults", {}).get("productsInfo")
        or []
    )

    products = []
    for item in products_info:
        normalized = normalize_products_api_item(item)
        if normalized:
            products.append(normalized)
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
    browser_preference = os.environ.get("WHOLEFOODS_SEARCH_BROWSER", "chromium").strip().lower()
    headless_env = os.environ.get("WHOLEFOODS_SEARCH_HEADLESS", "true").strip().lower()
    headless = headless_env not in {"0", "false", "no", "off"}

    if browser_preference not in {"chromium", "chrome"}:
        print(f'Unknown WHOLEFOODS_SEARCH_BROWSER="{browser_preference}"; falling back to chromium.')
        browser_preference = "chromium"

    if browser_preference == "chrome" and GOOGLE_CHROME_EXECUTABLE.exists():
        try:
            browser = playwright.chromium.launch(
                executable_path=str(GOOGLE_CHROME_EXECUTABLE),
                headless=headless,
                slow_mo=35,
            )
            mode = "headless" if headless else "visible"
            print(f"Using installed Google Chrome in {mode} mode.")
            return browser
        except Exception as e:
            print(f"Installed Google Chrome launch failed, falling back to Chromium: {e}")

    mode = "headless" if headless else "visible"
    print(f"Using Playwright Chromium in {mode} mode.")
    return playwright.chromium.launch(headless=headless, slow_mo=35)


def wait_for_store_modal(page, timeout_ms: int = 8000):
    remaining = timeout_ms

    while remaining > 0:
        modal = get_store_modal(page)
        if modal is not None:
            return modal
        page.wait_for_timeout(250)
        remaining -= 250

    return None


def open_update_location_modal(page, allow_direct_modal_fallback: bool = True) -> None:
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
        if not allow_direct_modal_fallback:
            raise RuntimeError("Could not open the store modal from the page-level location launcher.")
        print("Could not open the store modal from the deals page; opening the direct store modal URL.")
        page.goto(STORE_MODAL_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(1800)
        dismiss_popups(page)


def run_store_selection_cycle(
    page,
    store: Optional[dict],
    cycle_number: int,
    close_after_selection: bool = False,
    allow_direct_modal_fallback: bool = True,
) -> None:
    target_store = store_display_name(store)
    target_searches = store_search_terms(store)
    target_pattern = store_selected_pattern(store)
    used_direct_modal_fallback = False
    if cycle_number > 1:
        print(f"Opening the location modal again for store selection pass {cycle_number}.")

    modal = wait_for_store_modal(page, timeout_ms=500)
    if modal is None:
        open_update_location_modal(page, allow_direct_modal_fallback=allow_direct_modal_fallback)
        used_direct_modal_fallback = "modalView=true" in (page.url or "")
        page.wait_for_timeout(600)
        modal = wait_for_store_modal(page, timeout_ms=8000)
    selector_available = wait_for_store_iframe_text(
        page,
        rf"Find a Whole Foods Market|Find a store near you|Locate a store|{target_pattern}",
        timeout_ms=1500,
    ) if modal is None else True
    if modal is None and not selector_available:
        raise RuntimeError(
            "Location modal did not appear, so the scraper refused to use the page-level search box."
        )

    search_ok = False
    for query in target_searches:
        if len(target_searches) > 1:
            print(f'Trying store search query "{query}" for {target_store}.')

        if not fill_store_search_input_in_iframe(page, query):
            continue

        search_ok = wait_for_store_iframe_text(
            page,
            target_pattern,
            timeout_ms=12000,
        )
        if search_ok:
            break

    if not search_ok:
        raise RuntimeError(f"Could not search for {target_store} inside the location modal.")

    made_store = click_make_this_my_store_for_store(page, store)
    if not made_store:
        raise RuntimeError(f'Could not click the {target_store} store CTA.')

    print(f"Clicked the {target_store} store CTA on pass {cycle_number}; waiting for the modal to auto-close.")
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
            print(f'Pass {cycle_number} did not auto-close; trying modal close fallbacks.')
            closed = False
            if click_close_in_store_modal(page):
                page.wait_for_timeout(1800)
                dismiss_popups(page)
                closed = wait_for_store_modal_to_disappear(page, timeout_ms=5000)

            if not closed and click_center_of_viewport(page):
                page.wait_for_timeout(2000)
                dismiss_popups(page)
                closed = wait_for_store_modal_to_disappear(page, timeout_ms=5000)

            if not closed and used_direct_modal_fallback:
                print(f"Pass {cycle_number} used the direct modal fallback; reopening the deals page to verify the new store context.")
                page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
                page.wait_for_timeout(2500)
                dismiss_popups(page)
                closed = True

            if not closed:
                raise RuntimeError(f"Clicked the {target_store} store CTA, but the location modal never auto-closed.")

        page.wait_for_timeout(1200)
        dismiss_popups(page)

    selected_store = get_selected_store_text(page)
    print(f"Selected store text after pass {cycle_number}: {selected_store or '<empty>'}")


def set_store_from_search_page(page, store: Optional[dict] = None) -> None:
    target_store = store_display_name(store)
    target_pattern = store_selected_pattern(store)
    store_flow = os.environ.get("WHOLEFOODS_SEARCH_STORE_FLOW", "hybrid").strip().lower()
    if store_flow not in {"page", "direct", "hybrid"}:
        print(f'Unknown WHOLEFOODS_SEARCH_STORE_FLOW="{store_flow}"; falling back to page.')
        store_flow = "page"

    print(f"Opening sales search page for store setup: {SEARCH_DEALS_URL}")

    last_error = None
    for attempt in range(2):
        try:
            if attempt > 0:
                print(f"Retrying page-level store flow after the page still did not show {target_store}.")

            if store_flow == "direct":
                raise RuntimeError("Direct store flow requested, but the page-level flow is now preferred for probes.")

            page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(INITIAL_PAGE_SETTLE_MS)
            dismiss_popups(page)
            run_store_selection_cycle(
                page,
                store,
                cycle_number=attempt + 1,
                allow_direct_modal_fallback=(store_flow in {"page", "hybrid"}),
            )

            selected_store = get_selected_store_text(page)
            print(f"Selected store text after verification: {selected_store or '<empty>'}")
            page_store_context = get_page_store_context(page)
            if page_store_context:
                print(
                    "Page store context after verification: "
                    f"id={page_store_context.get('store_id') or '<empty>'} "
                    f"name={page_store_context.get('store_name') or '<empty>'} "
                    f"zip={page_store_context.get('postal_code') or '<empty>'}"
                )

            if wait_for_selected_store(page, store, timeout_ms=12000):
                print(f"Confirmed selected store: {target_store}.")
                try:
                    home_response = page.goto(WHOLE_FOODS_HOME_URL, wait_until="domcontentloaded", timeout=60000)
                    home_status = home_response.status if home_response else None
                    print(f"Warmed Whole Foods home page after store selection; status={home_status}.")
                    page.wait_for_timeout(2200)
                    dismiss_popups(page)
                    page.goto(SEARCH_DEALS_URL, wait_until="domcontentloaded")
                    page.wait_for_timeout(2500)
                    dismiss_popups(page)
                except Exception as exc:
                    print(f"Home-page warm step after store selection did not complete cleanly: {exc}")
                return

            last_error = RuntimeError(
                f'The page still did not show "{target_store}" as the selected store after the page-level store flow.'
            )
        except Exception as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise RuntimeError("Store selection failed before scraping could begin.")


def crawl_current_sort(page, products_by_asin: dict, sort_label: str) -> dict:
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
    return {"added_count": total_added_for_sort, "request_count": 0}


def crawl_current_sort_via_network(
    page,
    products_by_asin: dict,
    sort_label: str,
    sort_rank: str,
    extra_rh_values: Optional[list[str]] = None,
) -> dict:
    page.wait_for_timeout(1200)
    dismiss_popups(page)
    wait_for_search_results(page, timeout_ms=10000)

    initial_added = merge_products_from_current_page(page, products_by_asin)
    print(f"{sort_label}: initial products captured {len(products_by_asin)} (+{initial_added}) from page payload")

    html = page.content()
    offer_listing_discriminator = extract_offer_listing_discriminator(html)
    if not offer_listing_discriminator:
        raise RuntimeError(f'Could not find the offer listing discriminator for "{sort_label}".')

    total_added_for_sort = initial_added
    stale_batches = 0
    successful_batches = 0
    expected_total = None
    previous_batch_signature = None
    result_window_limit = parse_positive_int_env("WHOLEFOODS_SEARCH_RESULT_WINDOW_LIMIT") or NETWORK_RESULT_WINDOW_LIMIT

    for offset in range(0, SEARCH_RESULTS_PAGE_SIZE * MAX_ACTION_ROUNDS, SEARCH_RESULTS_PAGE_SIZE):
        if result_window_limit and offset >= result_window_limit:
            print(
                f"{sort_label}: stopping network fetch before offset {offset} "
                f"because the Whole Foods result window is capped at {result_window_limit}."
            )
            break

        if expected_total is not None and offset >= expected_total:
            print(
                f"{sort_label}: stopping network fetch because the requested offset "
                f"{offset} reached the reported total of {expected_total}."
            )
            break

        payload, asins, parsed_total, used_batch_size, rsi_status = fetch_rsi_payload_with_tail_fallback(
            page,
            offer_listing_discriminator,
            sort_label,
            sort_rank,
            offset,
            extra_rh_values=extra_rh_values,
        )
        if not payload and not asins:
            if successful_batches > 0 and rsi_status in {404, 429, 500, 502, 503, 504}:
                print(
                    f"{sort_label}: stopping network fetch after HTTP {rsi_status} at offset {offset}; "
                    "treating it as the end of paginated results."
                )
                break
            raise RuntimeError(f'{sort_label}: rsi/search returned HTTP {rsi_status} at offset {offset}.')

        expected_total = parsed_total or expected_total
        if not asins:
            print(f"{sort_label}: stopping network fetch because rsi/search returned no ASINs at offset {offset}.")
            break

        batch_signature = tuple(asins)
        if batch_signature == previous_batch_signature:
            print(f"{sort_label}: stopping network fetch because rsi/search repeated the same ASIN batch at offset {offset}.")
            break
        previous_batch_signature = batch_signature

        products_url = build_products_url(offer_listing_discriminator, asins)
        product_status = 0
        product_text = ""
        for attempt in range(1, NETWORK_HTTP_RETRY_LIMIT + 1):
            product_status, product_text = fetch_text_via_page(page, products_url)
            if product_status == 200:
                break
            if attempt < NETWORK_HTTP_RETRY_LIMIT:
                print(
                    f"{sort_label}: products API returned HTTP {product_status} at offset {offset}; "
                    f"retrying ({attempt}/{NETWORK_HTTP_RETRY_LIMIT - 1})."
                )
                page.wait_for_timeout(1200)
        if product_status != 200:
            if successful_batches > 0 and product_status in {404, 429, 500, 502, 503, 504}:
                print(
                    f"{sort_label}: stopping network fetch after products API HTTP {product_status} at offset {offset}; "
                    "treating it as the end of paginated results."
                )
                break
            raise RuntimeError(f'{sort_label}: products API returned HTTP {product_status} at offset {offset}.')

        try:
            product_items = json.loads(product_text)
        except Exception as exc:
            raise RuntimeError(f'{sort_label}: could not parse products JSON at offset {offset}: {exc}')

        before_count = len(products_by_asin)
        normalized_count = 0
        for item in product_items or []:
            normalized = normalize_products_api_item(item)
            if not normalized:
                continue
            normalized_count += 1
            asin = normalized["asin"]
            if asin not in products_by_asin:
                total_added_for_sort += 1
            products_by_asin[asin] = merge_product(products_by_asin.get(asin), normalized)

        batch_new = len(products_by_asin) - before_count
        print(
            f"{sort_label} network batch offset={offset}: "
            f"rsi_asins={len(asins)} products={normalized_count} total={len(products_by_asin)} "
            f"(+{batch_new}) expected_total={expected_total or '?'} size={used_batch_size}"
        )
        successful_batches += 1

        if batch_new <= 0:
            stale_batches += 1
        else:
            stale_batches = 0

        if len(asins) < used_batch_size:
            print(f"{sort_label}: stopping network fetch because the batch was short at offset {offset}.")
            break

        if result_window_limit and (offset + used_batch_size) >= result_window_limit:
            print(
                f"{sort_label}: stopping network fetch after offset {offset} "
                f"because the next page would move past the Whole Foods result window cap of {result_window_limit}."
            )
            break

        if expected_total is not None and (offset + len(asins)) >= expected_total:
            print(
                f"{sort_label}: stopping network fetch because offset {offset} + batch size {len(asins)} "
                f"reached the reported total of {expected_total}."
            )
            break

        if stale_batches >= NETWORK_STALE_BATCH_LIMIT:
            print(f"{sort_label}: stopping network fetch because consecutive batches added no new products.")
            break

    final_added = merge_products_from_current_page(page, products_by_asin)
    total_added_for_sort += final_added
    print(f"{sort_label}: final settle captured +{final_added}; total unique products now {len(products_by_asin)}")
    return {"added_count": total_added_for_sort, "request_count": successful_batches}


def discover_search_deals(store: Optional[dict] = None) -> dict:
    products_by_asin = {}
    captured_batch_urls = []
    replayed_network_batches = 0
    sort_runs_summary = []
    target_store_id = str((store or {}).get("id") or "")
    target_store_name = store_display_name(store)
    target_pattern = store_selected_pattern(store)
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

    collection_mode = os.environ.get("WHOLEFOODS_SEARCH_COLLECTION_MODE", "dom").strip().lower()
    if collection_mode not in {"dom", "network"}:
        print(f'Unknown WHOLEFOODS_SEARCH_COLLECTION_MODE="{collection_mode}"; falling back to dom.')
        collection_mode = "dom"
    if collection_mode == "network":
        print("Running Whole Foods search in network collection mode: replaying rsi/search and products APIs after the page loads.")

    max_runs = parse_positive_int_env("WHOLEFOODS_SEARCH_MAX_RUNS")
    if max_runs:
        print(f"Limiting Whole Foods search to the first {max_runs} run(s) for this session.")
        remaining = max_runs
        limited_run_plans = []
        for plan in run_plans:
            if remaining <= 0:
                break
            sorts = plan["sorts"][:remaining]
            if not sorts:
                continue
            limited_plan = dict(plan)
            limited_plan["sorts"] = sorts
            limited_run_plans.append(limited_plan)
            remaining -= len(sorts)
        run_plans = limited_run_plans

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
        set_store_from_search_page(page, store)

        if not wait_for_selected_store(page, store, timeout_ms=6000):
            print("Pre-scroll store check failed; retrying the store modal flow.")
            set_store_from_search_page(page, store)
            if not wait_for_selected_store(page, store, timeout_ms=6000):
                raise RuntimeError(
                    f'The page still did not show "{target_store_name}" as the selected store before deals scrolling began.'
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
                    open_search_run(page, run_label, sort_label, sort_rank, extra_rh_values=rh_values, store=store)
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
                    write_search_partial_checkpoint(
                        products_by_asin,
                        len(captured_batch_urls) + replayed_network_batches,
                        sort_runs_summary,
                    )
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
                if collection_mode == "network":
                    crawl_summary = crawl_current_sort_via_network(
                        page,
                        products_by_asin,
                        run_label,
                        sort_rank,
                        extra_rh_values=rh_values,
                    )
                    replayed_network_batches += crawl_summary["request_count"]
                else:
                    crawl_summary = crawl_current_sort(page, products_by_asin, run_label)
                new_products_found = len(products_by_asin) - before_sort_count
                sort_runs_summary.append(
                    {
                        "filter_label": filter_label,
                        "rh_values": rh_values,
                        "sort_label": sort_label,
                        "sort_rank": sort_rank,
                        "run_label": run_label,
                        "new_products_found": new_products_found,
                        "captured_events": crawl_summary["added_count"],
                        "collection_request_count": crawl_summary["request_count"],
                        "total_products_after_sort": len(products_by_asin),
                        "skipped": False,
                    }
                )
                write_search_partial_checkpoint(
                    products_by_asin,
                    len(captured_batch_urls) + replayed_network_batches,
                    sort_runs_summary,
                )
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
    for product in ordered_products:
        product["available_store_ids"] = [target_store_id] if target_store_id else []
        product["source_store_id"] = target_store_id or None
        product["source_store_name"] = target_store_name

    return {
        "search_url": SEARCH_DEALS_URL,
        "product_count": len(ordered_products),
        "network_batch_count": len(captured_batch_urls) + replayed_network_batches,
        "sort_runs": sort_runs_summary,
        "store_id": target_store_id,
        "store_name": target_store_name,
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
