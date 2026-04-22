import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from discover_search_deals import discover_search_deals


@contextmanager
def patched_env(updates: dict[str, str]):
    original = {}
    for key, value in updates.items():
        original[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, old_value in original.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark Whole Foods search DOM crawl vs network replay on the same store."
    )
    parser.add_argument("--store-id", default="10160")
    parser.add_argument("--store-name", default="Columbus Circle")
    parser.add_argument("--browser", default="chrome", choices=["chrome", "chromium"])
    parser.add_argument("--search-mode", default="fast", choices=["fast", "full"])
    parser.add_argument("--max-runs", type=int, default=1)
    parser.add_argument("--output", default="logs/wfm_search_collection_benchmark.json")
    return parser


def run_mode(mode: str, store: dict, args) -> dict:
    started_at = time.monotonic()
    with patched_env(
        {
            "WHOLEFOODS_SEARCH_COLLECTION_MODE": mode,
            "WHOLEFOODS_SEARCH_MODE": args.search_mode,
            "WHOLEFOODS_SEARCH_MAX_RUNS": str(args.max_runs),
            "WHOLEFOODS_SEARCH_BROWSER": args.browser,
        }
    ):
        result = discover_search_deals(store=store)

    elapsed_s = round(time.monotonic() - started_at, 3)
    return {
        "mode": mode,
        "elapsed_s": elapsed_s,
        "product_count": result["product_count"],
        "network_batch_count": result["network_batch_count"],
        "sort_runs": result["sort_runs"],
    }


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = {"id": args.store_id, "name": args.store_name}
    modes = ["dom", "network"]
    results = []

    for mode in modes:
        print()
        print(f"=== Benchmarking {mode.upper()} mode ===")
        results.append(run_mode(mode, store, args))

    payload = {
        "store": store,
        "search_mode": args.search_mode,
        "max_runs": args.max_runs,
        "browser": args.browser,
        "results": results,
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print()
    print(f"Wrote {output_path}")
    for row in results:
        print(
            f"{row['mode']:>7}: elapsed={row['elapsed_s']}s "
            f"products={row['product_count']} "
            f"requests={row['network_batch_count']}"
        )

    if len(results) == 2 and results[0]["elapsed_s"] > 0 and results[1]["elapsed_s"] > 0:
        dom = next((row for row in results if row["mode"] == "dom"), None)
        network = next((row for row in results if row["mode"] == "network"), None)
        if dom and network:
            speedup = round(dom["elapsed_s"] / network["elapsed_s"], 2) if network["elapsed_s"] else None
            print(
                f"Speedup: {speedup}x faster in network mode"
                if speedup
                else "Speedup: unavailable"
            )


if __name__ == "__main__":
    main()
