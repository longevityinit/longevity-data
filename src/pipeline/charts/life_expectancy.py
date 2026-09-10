import argparse
import csv
import html
import json
import re
import shutil
import sys
from pathlib import Path

import requests
import yaml

script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
CHART_SLUG = DATASET_NAME.replace("_", "-")
ROOT_PATH = Path(__file__).resolve().parents[3]
TEMPLATE_FILE = script_path.parent / "templates" / "line_chart.html"


sys.path.append(str(ROOT_PATH / "src/pipeline"))

from utils.paths import get_output_root
from utils.storage import sync_to_storage

PLOT_VERSION = "0.6.16"
PLOT_URL = f"https://cdn.jsdelivr.net/npm/@observablehq/plot@{PLOT_VERSION}/dist/plot.umd.min.js"
PLOT_FILENAME = f"plot-{PLOT_VERSION}.umd.min.js"

D3_VERSION = "7.9.0"
D3_URL = f"https://cdn.jsdelivr.net/npm/d3@{D3_VERSION}/dist/d3.min.js"
D3_FILENAME = f"d3-{D3_VERSION}.min.js"


def ensure_vendor_assets(vendor_dir: Path):
    vendor_dir.mkdir(parents=True, exist_ok=True)
    for filename, url in [(D3_FILENAME, D3_URL), (PLOT_FILENAME, PLOT_URL)]:
        dest = vendor_dir / filename
        if dest.exists():
            continue
        print(f"Downloading {filename}...")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        dest.write_bytes(r.content)
        print(f"Saved {dest.stat().st_size // 1024} kB to {dest}")


def public_column_name(name: str) -> str:
    """Strip OWID's per-dataset disambiguation suffix (e.g. life_expectancy_0 → life_expectancy)."""
    return re.sub(r"_\d+$", "", name)


def write_data_csv(src: Path, dst_dir: Path, rename: dict[str, str], out_filename: str) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / out_filename
    rows = []
    with open(src, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            new_row = {}
            for k, v in row.items():
                key = rename.get(k, k)
                if k not in ("entity", "code", "year"):
                    try:
                        v = f"{float(v):.2f}" if v else ""
                    except ValueError:
                        pass
                new_row[key] = v
            rows.append(new_row)
    with open(dst, "w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
    return dst


def write_html(meta: dict, dst_dir: Path, value_col: str, data_filename: str) -> Path:
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    defaults = meta.get("default_selection") or ["World"]
    citation = meta["columns"][0].get("citation_short") or meta.get("citation", "")
    page = (template
            .replace("__CHART_TITLE__", html.escape(meta.get("title", "")))
            .replace("__CITATION__", html.escape(citation))
            .replace("__VALUE_COL__", value_col)
            .replace("__DEFAULT_SELECTION__", json.dumps(defaults))
            .replace("__DATA_FILENAME__", data_filename))
    dst = dst_dir / "index.html"
    dst.write_text(page, encoding="utf-8")
    return dst


def main():
    parser = argparse.ArgumentParser(description="Run this pipeline stage.")
    parser.add_argument("--local", action="store_true", help="Write local artifacts without uploading to storage.")
    args = parser.parse_args()
    print(f"Building chart for {CHART_SLUG}...")
    output_root = get_output_root(ROOT_PATH, local=args.local)
    data_path = output_root / "data"
    charts_path = output_root / "charts"
    lib_dir = charts_path / "lib"
    vendor_dir = charts_path / "vendor"
    std_dir = data_path / "standardised" / "owid" / DATASET_NAME
    src_csv = std_dir / f"{DATASET_NAME}.csv"
    src_meta = std_dir / f"{DATASET_NAME}.meta.yaml"

    for path in (src_csv, src_meta):
        if not path.exists():
            mode = " --local" if args.local else ""
            print(f"Missing: {path}\nRun python src/pipeline/standardise/owid/{DATASET_NAME}.py{mode} first.")
            sys.exit(1)

    with open(src_meta, encoding="utf-8") as f:
        meta = yaml.safe_load(f)

    ensure_vendor_assets(vendor_dir)
    lib_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT_PATH / "src/charts/longevityplot.js", lib_dir / "longevityplot.js")

    src_col = meta["columns"][0]["name"]
    public_col = public_column_name(src_col)
    data_filename = f"{CHART_SLUG}_tli.csv"

    chart_dir = charts_path / CHART_SLUG
    write_data_csv(src_csv, chart_dir, {src_col: public_col}, data_filename)
    write_html(meta, chart_dir, public_col, data_filename)
    print(f"Chart written to {chart_dir}")

    sync_to_storage({
        vendor_dir / D3_FILENAME:       f"charts/vendor/{D3_FILENAME}",
        vendor_dir / PLOT_FILENAME:      f"charts/vendor/{PLOT_FILENAME}",
        lib_dir / "longevityplot.js":    "charts/lib/longevityplot.js",
        chart_dir / data_filename:       f"charts/{CHART_SLUG}/{data_filename}",
        chart_dir / "index.html":        f"charts/{CHART_SLUG}/index.html",
    }, local=args.local)
    print(f"Chart build complete for {CHART_SLUG}!")


if __name__ == "__main__":
    main()
