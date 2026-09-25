import json
import hashlib
from datetime import datetime, timezone
import os
import shutil
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DIST_DIR = BASE_DIR / "dist"
STATIC_DIR = BASE_DIR / "static"


def load_dotenv_file() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_dotenv_file()
from app import app
ROOT_STATIC_FILES = [
    "manifest.webmanifest",
    "service-worker.js",
]
REPORT_FILES = [
    "catalog_quality_audit.html",
    "catalog_recovery.json",
    "sale_coverage.json",
    "catalog_quality_audit.json",
    "failed_products_review_queue.json",
]
DATA_FILES = [
    "flyer_products.json",
    "flyer_report.json",
    "discovered_products.json",
    "discovered_recommendations.json",
    "captured_batches.json",
    "search_deals_products.json",
    "search_deals_report.json",
    "target_deals_products.json",
    "target_deals_report.json",
    "hmart_deals_products.json",
    "hmart_deals_report.json",
    "combined_products.json",
    "combined_report.json",
    "discovered_taxonomy.json",
    "taxonomy_classification_cache.json",
    "taxonomy_ai_report.json",
    "subcategory_ai_metadata.json",
    "subcategory_ai_report.json",
    "fixes_to_deploy.json",
    "flyer_vs_all_deals_report.json",
]
ROUTES = {
    "/": DIST_DIR / "index.html",
    "/meal-plan/": DIST_DIR / "meal-plan" / "index.html",
}


def ensure_successful_response(client, route: str) -> str:
    response = client.get(route)
    if response.status_code != 200:
        raise RuntimeError(f"Could not render {route}: status {response.status_code}")
    return response.get_data(as_text=True)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(line.rstrip() for line in text.splitlines()) + "\n", encoding="utf-8")


def copy_static_assets() -> None:
    target = DIST_DIR / "static"
    shutil.copytree(STATIC_DIR, target, dirs_exist_ok=True)

    for filename in ROOT_STATIC_FILES:
        src = STATIC_DIR / filename
        if src.exists():
            shutil.copy2(src, DIST_DIR / filename)


def copy_data_files() -> None:
    data_dir = DIST_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    for filename in DATA_FILES:
        src = BASE_DIR / filename
        if src.exists():
            (data_dir / filename).write_text(json.dumps(json.loads(src.read_text()), ensure_ascii=False, separators=(',', ':')))


def copy_report_files() -> None:
    reports_dir = DIST_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    for filename in REPORT_FILES:
        src = BASE_DIR / "reports" / filename
        if src.exists():
            if src.suffix == '.json':
                (reports_dir / filename).write_text(json.dumps(json.loads(src.read_text()), ensure_ascii=False, separators=(',', ':')))
            else:
                shutil.copy2(src, reports_dir / filename)

    concepts_src = BASE_DIR / "reports" / "ui_concepts"
    if concepts_src.exists():
        shutil.copytree(concepts_src, reports_dir / "ui_concepts", dirs_exist_ok=True)


def write_metadata() -> None:
    copied_report_files = [name for name in REPORT_FILES if (BASE_DIR / "reports" / name).exists()]
    concepts_src = BASE_DIR / "reports" / "ui_concepts"
    if concepts_src.exists():
        copied_report_files.extend(
            str(Path("ui_concepts") / path.relative_to(concepts_src))
            for path in sorted(concepts_src.rglob("*"))
            if path.is_file()
        )
    metadata = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "catalog_sha256": hashlib.sha256((BASE_DIR / "combined_products.json").read_bytes()).hexdigest(),
        "routes": sorted(ROUTES.keys()),
        "copied_data_files": [name for name in DATA_FILES if (BASE_DIR / name).exists()],
        "copied_report_files": copied_report_files,
        "root_static_files": [name for name in ROOT_STATIC_FILES if (STATIC_DIR / name).exists()],
    }
    write_text(DIST_DIR / "build-meta.json", json.dumps(metadata, indent=2))


def main() -> None:
    from app import load_combined_products
    from scripts.sale_coverage import reconcile
    coverage_path = BASE_DIR / 'reports' / 'sale_coverage.json'
    if coverage_path.exists():
        coverage = reconcile(json.loads(coverage_path.read_text()), load_combined_products())
        coverage_path.write_text(json.dumps(coverage, indent=2) + '\n')
        if coverage['missing_from_catalog']:
            raise RuntimeError('Advertised promotions disappeared from shopper catalog: ' + str(coverage['missing_from_catalog']))
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
    copy_report_files()
    write_metadata()
    oversized = [str(p.relative_to(DIST_DIR)) for p in DIST_DIR.rglob('*') if p.is_file() and p.stat().st_size > 25 * 1024 * 1024]
    if oversized:
        raise RuntimeError("Cloudflare asset size limit exceeded: " + ", ".join(oversized))
    print(f"\nStatic site built at {DIST_DIR}")


if __name__ == "__main__":
    main()
