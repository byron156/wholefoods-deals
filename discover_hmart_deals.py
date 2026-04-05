import json
import re
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

import requests


HMART_BASE_URL = "https://www.hmart.com"
HMART_START_URLS = [
    "https://www.hmart.com/weekly-sale?map=productClusterNames&order=OrderByBestDiscountDESC",
    "https://www.hmart.com/237?map=productClusterIds&page=1",
    "https://www.hmart.com/166?map=productClusterIds&order=OrderByBestDiscountDESC&page=1",
]
PAGE_SIZE = 30


def normalize_text_key(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def build_page_url(base_url: str, page_number: int) -> str:
    parts = urlsplit(base_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query["page"] = [str(page_number)]
    new_query = urlencode(query, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def extract_storefront_cache(html: str) -> dict[str, Any]:
    for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.DOTALL):
        script_text = match.group(1)
        if "Product:sp-" in script_text and "productSearch" in script_text:
            return json.loads(script_text)
    raise ValueError("Could not find H Mart storefront cache JSON.")


def root_product_search(cache: dict[str, Any]) -> dict[str, Any]:
    for key, value in cache.items():
        if (
            key.startswith("$ROOT_QUERY.productSearch(")
            and isinstance(value, dict)
            and "products" in value
        ):
            return value
    raise ValueError("Could not find H Mart product search payload.")


def resolve_ref_id(ref: Any) -> Optional[str]:
    if isinstance(ref, dict):
        return ref.get("id") or ref.get("__ref")
    if isinstance(ref, str):
        return ref
    return None


def compute_discount_text(list_price: float, sale_price: float) -> Optional[str]:
    if list_price <= 0 or sale_price >= list_price:
        return None
    percent_off = round(100 - (sale_price / list_price * 100))
    return f"{percent_off}% off"


def parse_hmart_product(cache: dict[str, Any], product_ref: Any) -> Optional[dict[str, Any]]:
    product_id = resolve_ref_id(product_ref)
    if not product_id or product_id not in cache:
        return None

    product = cache[product_id]
    item_key = next((key for key in product.keys() if key.startswith("items(")), None)
    if not item_key or not product.get(item_key):
        return None

    item_id = resolve_ref_id(product[item_key][0])
    if not item_id or item_id not in cache:
        return None

    item = cache[item_id]
    sellers = item.get("sellers") or []
    if not sellers:
        return None

    seller_id = resolve_ref_id(sellers[0])
    if not seller_id or seller_id not in cache:
        return None

    seller = cache[seller_id]
    offer_id = resolve_ref_id(seller.get("commertialOffer"))
    if not offer_id or offer_id not in cache:
        return None

    offer = cache[offer_id]
    sale_price = offer.get("Price")
    list_price = offer.get("ListPrice")

    if sale_price is None or list_price is None:
        return None
    if float(list_price) <= float(sale_price):
        return None

    images = item.get("images") or []
    image_url = None
    if images:
        image_id = resolve_ref_id(images[0])
        image = cache.get(image_id, {}) if image_id else {}
        image_url = image.get("imageUrl") or image.get("imageLabel")

    categories = []
    category_json = product.get("categories")
    if isinstance(category_json, dict):
        categories = category_json.get("json") or []

    return {
        "asin": f"hmart:{product.get('productId') or normalize_text_key(product.get('productName'))}",
        "name": product.get("productName") or item.get("name"),
        "brand": product.get("brand"),
        "variation": None,
        "image": image_url,
        "url": urljoin(HMART_BASE_URL, product.get("link") or ""),
        "current_price": f"${float(sale_price):.2f}",
        "basis_price": f"${float(list_price):.2f}",
        "prime_price": None,
        "discount": compute_discount_text(float(list_price), float(sale_price)),
        "unit_price": None,
        "retailer": "H Mart",
        "retail_source_url": None,
        "categories": categories,
        "available_quantity": offer.get("AvailableQuantity"),
    }


def scrape_hmart_listing(start_url: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    session = requests.Session()
    session.headers.update(
        {
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )

    collected: dict[str, dict[str, Any]] = {}
    records_filtered = None
    page_number = 1
    pages_scraped = 0

    while True:
        page_url = build_page_url(start_url, page_number)
        response = session.get(page_url, timeout=30)
        response.raise_for_status()
        cache = extract_storefront_cache(response.text)
        search_payload = root_product_search(cache)
        product_refs = search_payload.get("products") or []

        if records_filtered is None:
            records_filtered = search_payload.get("recordsFiltered") or 0

        if not product_refs:
            break

        page_new_count = 0
        for product_ref in product_refs:
            parsed = parse_hmart_product(cache, product_ref)
            if not parsed:
                continue
            parsed["retail_source_url"] = page_url
            key = parsed["asin"]
            if key not in collected:
                collected[key] = parsed
                page_new_count += 1

        pages_scraped += 1
        print(
            f"H Mart page {page_number}: kept {page_new_count} new sale items "
            f"(total {len(collected)})"
        )

        if len(product_refs) < PAGE_SIZE:
            break
        if page_new_count == 0 and page_number > 1:
            break
        if records_filtered and page_number * PAGE_SIZE >= int(records_filtered):
            break

        page_number += 1

    return list(collected.values()), {
        "start_url": start_url,
        "records_filtered": records_filtered or 0,
        "pages_scraped": pages_scraped,
        "product_count": len(collected),
    }


def discover_hmart_deals() -> dict[str, Any]:
    combined: dict[str, dict[str, Any]] = {}
    runs = []

    for start_url in HMART_START_URLS:
        print(f"Scraping H Mart listing: {start_url}")
        products, report = scrape_hmart_listing(start_url)
        runs.append(report)
        for product in products:
            combined.setdefault(product["asin"], product)

    ordered = sorted(combined.values(), key=lambda item: normalize_text_key(item.get("name")))
    return {
        "source_urls": HMART_START_URLS,
        "product_count": len(ordered),
        "runs": runs,
        "products": ordered,
    }


if __name__ == "__main__":
    result = discover_hmart_deals()
    print(json.dumps(result["runs"], indent=2))
    print(f"H Mart total unique products: {result['product_count']}")
