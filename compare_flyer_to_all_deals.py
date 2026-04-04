import json
import os
import re
from difflib import SequenceMatcher

from app import fetch_products, load_all_deals

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "flyer_vs_all_deals_report.json")
SEARCH_DEALS_FILE = os.path.join(BASE_DIR, "search_deals_products.json")


def load_search_deals():
    print("Looking for search-deals file at:", SEARCH_DEALS_FILE)

    try:
        with open(SEARCH_DEALS_FILE, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
    except FileNotFoundError:
        print("Search-deals file not found.")
        return []

    if not isinstance(raw_products, list):
        print("Search-deals file is not a list; ignoring it.")
        return []

    print("Loaded", len(raw_products), "search-deals products")
    return raw_products


def normalize_asin_list(asins):
    if not asins:
        return []
    return [str(a).strip() for a in asins if a and str(a).strip()]


def normalize_text(text):
    if not text:
        return ""
    text = text.lower().strip()

    replacements = {
        "&": " and ",
        "–": " ",
        "-": " ",
        "/": " ",
        ",": " ",
        ".": " ",
        " oz ": " ",
        " ounce ": " ",
        " ounces ": " ",
        " lb ": " ",
        " ea ": " ",
        " ct ": " ",
        " pk ": " ",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def token_set(text):
    return set(normalize_text(text).split())


def similarity(a, b):
    return SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def looks_grouped_promo(name):
    if not name:
        return False

    name_l = name.lower()

    grouped_signals = [
        " and ",
        ", ",
        " or ",
        "mix and match",
        "select ",
    ]

    return any(sig in name_l for sig in grouped_signals)


def looks_fresh_counter_item(name):
    if not name:
        return False

    name_l = name.lower()

    signals = [
        "apples",
        "asparagus",
        "avocados",
        "strawberries",
        "lemons",
        "oranges",
        "mangoes",
        "kiwis",
        "dates",
        "salmon",
        "shrimp",
        "ham",
        "lamb",
        "biscuits",
        "croissants",
        "cookies",
        "cake",
        "quiche",
        "sliced-in-house",
        "fresh ",
        "fillets",
        "/lb",
    ]

    return any(sig in name_l for sig in signals)


def best_fuzzy_match(flyer_name, candidate_names, threshold=0.72):
    flyer_norm = normalize_text(flyer_name)
    flyer_tokens = token_set(flyer_name)

    best = None
    best_score = 0.0

    for candidate in candidate_names:
        cand_norm = normalize_text(candidate)
        cand_tokens = token_set(candidate)

        seq_score = similarity(flyer_norm, cand_norm)

        overlap = 0.0
        if flyer_tokens and cand_tokens:
            overlap = len(flyer_tokens & cand_tokens) / max(1, len(flyer_tokens))

        score = max(seq_score, overlap)

        if score > best_score:
            best_score = score
            best = candidate

    if best_score >= threshold:
        return best, round(best_score, 3)

    return None, round(best_score, 3)


def build_asin_index(products):
    index = {}

    for product in products:
        asin = product.get("asin")
        if asin:
            index[str(asin).strip()] = product

    return index


def sample_products_for_asins(asins, products_by_asin, limit=50):
    rows = []

    for asin in sorted(asins)[:limit]:
        product = products_by_asin.get(asin, {})
        rows.append(
            {
                "asin": asin,
                "name": product.get("name"),
                "prime_price": product.get("prime_price"),
                "basis_price": product.get("basis_price"),
                "current_price": product.get("current_price"),
                "discount": product.get("discount"),
            }
        )

    return rows


def combine_deals_products(*product_lists):
    combined = []
    seen_asins = set()
    seen_name_keys = set()

    for products in product_lists:
        for product in products:
            asin = str(product.get("asin") or "").strip()
            name_key = normalize_text(product.get("name"))

            if asin:
                if asin in seen_asins:
                    continue
                seen_asins.add(asin)
                combined.append(product)
                continue

            if name_key:
                if name_key in seen_name_keys:
                    continue
                seen_name_keys.add(name_key)
                combined.append(product)

    return combined


def compare_flyer_against_dataset(dataset_label, flyer_products, comparison_products):
    comparison_asins = {p.get("asin") for p in comparison_products if p.get("asin")}
    comparison_names = [p.get("name", "") for p in comparison_products if p.get("name")]

    strict_matched = []
    no_asins = []
    fuzzy_name_matches = []
    grouped_promo_likely = []
    fresh_counter_likely = []
    true_strict_missing = []

    for product in flyer_products:
        name = product.get("name")
        asins = normalize_asin_list(product.get("asins", []))

        row = {
            "name": name,
            "asins": asins,
            "prime_price": product.get("prime_price"),
            "basis_price": product.get("basis_price"),
        }

        if not asins:
            match_name, score = best_fuzzy_match(name, comparison_names)
            row["best_name_match"] = match_name
            row["best_name_score"] = score

            if match_name:
                fuzzy_name_matches.append(row)
            else:
                no_asins.append(row)
            continue

        found_asin = any(asin in comparison_asins for asin in asins)
        if found_asin:
            strict_matched.append(row)
            continue

        match_name, score = best_fuzzy_match(name, comparison_names)
        row["best_name_match"] = match_name
        row["best_name_score"] = score

        if match_name:
            fuzzy_name_matches.append(row)
        elif looks_grouped_promo(name):
            grouped_promo_likely.append(row)
        elif looks_fresh_counter_item(name):
            fresh_counter_likely.append(row)
        else:
            true_strict_missing.append(row)

    return {
        "comparison_label": dataset_label,
        "comparison_count": len(comparison_products),
        "comparison_unique_asins": len(comparison_asins),
        "strict_asin_matched_count": len(strict_matched),
        "no_asins_count": len(no_asins),
        "fuzzy_name_match_count": len(fuzzy_name_matches),
        "grouped_promo_likely_count": len(grouped_promo_likely),
        "fresh_counter_likely_count": len(fresh_counter_likely),
        "true_strict_missing_count": len(true_strict_missing),
        "strict_asin_matched": strict_matched,
        "fuzzy_name_matches": fuzzy_name_matches,
        "grouped_promo_likely": grouped_promo_likely,
        "fresh_counter_likely": fresh_counter_likely,
        "no_asins": no_asins,
        "true_strict_missing": true_strict_missing,
    }


def compare_dataset_overlap(left_label, left_products, right_label, right_products):
    left_by_asin = build_asin_index(left_products)
    right_by_asin = build_asin_index(right_products)

    left_asins = set(left_by_asin)
    right_asins = set(right_by_asin)

    shared_asins = left_asins & right_asins
    left_only_asins = left_asins - right_asins
    right_only_asins = right_asins - left_asins

    return {
        "left_label": left_label,
        "right_label": right_label,
        "left_unique_asins": len(left_asins),
        "right_unique_asins": len(right_asins),
        "shared_asin_count": len(shared_asins),
        "left_only_count": len(left_only_asins),
        "right_only_count": len(right_only_asins),
        "left_only_examples": sample_products_for_asins(left_only_asins, left_by_asin),
        "right_only_examples": sample_products_for_asins(right_only_asins, right_by_asin),
    }


def print_comparison_summary(label, report):
    print(f"\n{label}:")
    print(f"  Comparison products: {report['comparison_count']}")
    print(f"  Strict ASIN matched: {report['strict_asin_matched_count']}")
    print(f"  Fuzzy name matches: {report['fuzzy_name_match_count']}")
    print(f"  Grouped promo likely misses: {report['grouped_promo_likely_count']}")
    print(f"  Fresh/counter likely misses: {report['fresh_counter_likely_count']}")
    print(f"  No ASIN rows: {report['no_asins_count']}")
    print(f"  True strict missing: {report['true_strict_missing_count']}")


def print_true_missing_rows(label, rows):
    if not rows:
        return

    print(f"\n{label} true strict missing:")
    for row in rows:
        print(
            "-",
            row["name"],
            "| ASINs:",
            ", ".join(row["asins"]) if row["asins"] else "(none)",
            "| Best name match:",
            row.get("best_name_match"),
            "| Score:",
            row.get("best_name_score"),
        )


def main():
    flyer_products = fetch_products()
    all_deals_products = load_all_deals()
    search_deals_products = load_search_deals()
    combined_deals_products = combine_deals_products(all_deals_products, search_deals_products)

    all_deals_comparison = compare_flyer_against_dataset(
        "all_deals",
        flyer_products,
        all_deals_products,
    )
    search_deals_comparison = compare_flyer_against_dataset(
        "search_deals",
        flyer_products,
        search_deals_products,
    )
    combined_deals_comparison = compare_flyer_against_dataset(
        "combined_deals",
        flyer_products,
        combined_deals_products,
    )
    deals_overlap = compare_dataset_overlap(
        "all_deals",
        all_deals_products,
        "search_deals",
        search_deals_products,
    )

    report = {
        "flyer_count": len(flyer_products),
        "all_deals_count": len(all_deals_products),
        "search_deals_count": len(search_deals_products),
        "combined_deals_count": len(combined_deals_products),
        "all_deals_comparison": all_deals_comparison,
        "search_deals_comparison": search_deals_comparison,
        "combined_deals_comparison": combined_deals_comparison,
        "all_deals_vs_search_deals": deals_overlap,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Flyer products: {len(flyer_products)}")
    print(f"All deals products: {len(all_deals_products)}")
    print(f"Search deals products: {len(search_deals_products)}")
    print(f"Combined deals products: {len(combined_deals_products)}")

    print_comparison_summary("Flyer vs all deals", all_deals_comparison)
    print_comparison_summary("Flyer vs search deals", search_deals_comparison)
    print_comparison_summary("Flyer vs combined deals", combined_deals_comparison)

    print("\nAll deals vs search deals:")
    print(f"  Shared ASINs: {deals_overlap['shared_asin_count']}")
    print(f"  All-deals only ASINs: {deals_overlap['left_only_count']}")
    print(f"  Search-deals only ASINs: {deals_overlap['right_only_count']}")
    print(f"Wrote report to: {OUTPUT_FILE}")

    print_true_missing_rows(
        "Flyer vs all deals",
        all_deals_comparison["true_strict_missing"],
    )
    print_true_missing_rows(
        "Flyer vs search deals",
        search_deals_comparison["true_strict_missing"],
    )
    print_true_missing_rows(
        "Flyer vs combined deals",
        combined_deals_comparison["true_strict_missing"],
    )


if __name__ == "__main__":
    main()
