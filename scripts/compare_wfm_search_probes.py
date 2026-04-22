import argparse
import json
from collections import Counter
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare two Whole Foods search probe artifacts.")
    parser.add_argument("left")
    parser.add_argument("right")
    return parser


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def summarize_urls(entries: list[dict], key: str = "url") -> Counter:
    counter = Counter()
    for entry in entries:
        url = entry.get(key) or ""
        counter[url] += 1
    return counter


def main() -> None:
    args = build_parser().parse_args()
    left = load(args.left)
    right = load(args.right)

    left_urls = set(summarize_urls(left.get("responses", [])).keys())
    right_urls = set(summarize_urls(right.get("responses", [])).keys())

    print("LEFT:", Path(args.left).name, "-", left.get("store"))
    print("RIGHT:", Path(args.right).name, "-", right.get("store"))
    print()

    print("Top-level:")
    for key in [
        "setup_error",
        "page_url",
        "next_data_found",
        "next_data_page_type",
        "next_data_product_count",
        "request_count",
        "response_count",
    ]:
        print(f"  {key}:")
        print(f"    left : {left.get(key)}")
        print(f"    right: {right.get(key)}")

    print()
    print("Only in left responses:")
    for url in sorted(left_urls - right_urls)[:40]:
        print(" ", url)

    print()
    print("Only in right responses:")
    for url in sorted(right_urls - left_urls)[:40]:
        print(" ", url)

    print()
    print("Shared responses with differing statuses:")
    right_status_by_url = {}
    for entry in right.get("responses", []):
        right_status_by_url.setdefault(entry.get("url"), set()).add(entry.get("status"))
    left_status_by_url = {}
    for entry in left.get("responses", []):
        left_status_by_url.setdefault(entry.get("url"), set()).add(entry.get("status"))

    for url in sorted(left_urls & right_urls):
        if left_status_by_url.get(url) != right_status_by_url.get(url):
            print(" ", url)
            print("   left :", sorted(left_status_by_url.get(url, [])))
            print("   right:", sorted(right_status_by_url.get(url, [])))


if __name__ == "__main__":
    main()
