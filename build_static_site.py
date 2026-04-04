import json
import shutil
from pathlib import Path

from app import app


BASE_DIR = Path(__file__).resolve().parent
DIST_DIR = BASE_DIR / "dist"
STATIC_DIR = BASE_DIR / "static"
DATA_FILES = [
    "flyer_products.json",
    "flyer_report.json",
    "discovered_products.json",
    "discovered_recommendations.json",
    "captured_batches.json",
    "search_deals_products.json",
    "search_deals_report.json",
    "combined_products.json",
    "combined_report.json",
    "flyer_vs_all_deals_report.json",
]
ROUTES = {
    "/": DIST_DIR / "index.html",
    "/flyer": DIST_DIR / "flyer" / "index.html",
    "/all-deals": DIST_DIR / "all-deals" / "index.html",
    "/search-deals": DIST_DIR / "search-deals" / "index.html",
    "/combined-products": DIST_DIR / "combined-products" / "index.html",
    "/newsletter": DIST_DIR / "newsletter" / "index.html",
    "/all-deals-newsletter": DIST_DIR / "all-deals-newsletter" / "index.html",
}


def ensure_successful_response(client, route: str) -> str:
    response = client.get(route)
    if response.status_code != 200:
        raise RuntimeError(f"Could not render {route}: status {response.status_code}")
    return response.get_data(as_text=True)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def copy_static_assets() -> None:
    target = DIST_DIR / "static"
    shutil.copytree(STATIC_DIR, target, dirs_exist_ok=True)


def copy_data_files() -> None:
    data_dir = DIST_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    for filename in DATA_FILES:
        src = BASE_DIR / filename
        if src.exists():
            shutil.copy2(src, data_dir / filename)


def write_metadata() -> None:
    metadata = {
        "routes": sorted(ROUTES.keys()),
        "copied_data_files": [name for name in DATA_FILES if (BASE_DIR / name).exists()],
    }
    write_text(DIST_DIR / "build-meta.json", json.dumps(metadata, indent=2))


def main() -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)

    DIST_DIR.mkdir(parents=True, exist_ok=True)

    with app.test_client() as client:
        for route, output_path in ROUTES.items():
            html = ensure_successful_response(client, route)
            write_text(output_path, html)
            print(f"Rendered {route} -> {output_path.relative_to(BASE_DIR)}")

    copy_static_assets()
    copy_data_files()
    write_metadata()
    print(f"\nStatic site built at {DIST_DIR}")


if __name__ == "__main__":
    main()
