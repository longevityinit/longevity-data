import sys
import csv
import html
import json
import re
import requests
from pathlib import Path
from dotenv import find_dotenv

script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
SOURCE = "owid"
CHART_SLUG = DATASET_NAME.replace("_", "-")
ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent
TEMPLATE_FILE = script_path.parent / "templates" / "line_chart.html"

print(f"Building chart for {CHART_SLUG}...")

sys.path.append(str(ROOT_PATH / "scripts"))
from utils.paths import CHART_LIB_DIR, chart_dir, standardised_dir
from utils.storage import sync_to_storage
from schemas import StandardisedMeta, read_yaml

PLOT_VERSION = "0.6.16"
PLOT_URL = f"https://cdn.jsdelivr.net/npm/@observablehq/plot@{PLOT_VERSION}/dist/plot.umd.min.js"
PLOT_FILENAME = f"plot-{PLOT_VERSION}.umd.min.js"

D3_VERSION = "7.9.0"
D3_URL = f"https://cdn.jsdelivr.net/npm/d3@{D3_VERSION}/dist/d3.min.js"
D3_FILENAME = f"d3-{D3_VERSION}.min.js"


def ensure_lib():
    CHART_LIB_DIR.mkdir(parents=True, exist_ok=True)
    for filename, url in [(D3_FILENAME, D3_URL), (PLOT_FILENAME, PLOT_URL)]:
        dest = CHART_LIB_DIR / filename
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


def write_html(meta: StandardisedMeta, dst_dir: Path, value_col: str, data_filename: str) -> Path:
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    defaults = meta.default_selection or ["World"]
    citation = meta.columns[0].citation_short or meta.citation or ""
    page = (template
            .replace("__CHART_TITLE__", html.escape(meta.title or ""))
            .replace("__CITATION__", html.escape(citation))
            .replace("__VALUE_COL__", value_col)
            .replace("__DEFAULT_SELECTION__", json.dumps(defaults))
            .replace("__DATA_FILENAME__", data_filename))
    dst = dst_dir / "index.html"
    dst.write_text(page, encoding="utf-8")
    return dst


def main():
    std_dir = standardised_dir(SOURCE, DATASET_NAME)
    src_csv = std_dir / f"{DATASET_NAME}.csv"
    src_meta_path = std_dir / f"{DATASET_NAME}.meta.yaml"

    for path in (src_csv, src_meta_path):
        if not path.exists():
            print(f"Missing: {path}\nRun scripts/standardise/{SOURCE}/{DATASET_NAME}.py first.")
            sys.exit(1)

    meta = read_yaml(StandardisedMeta, src_meta_path)

    ensure_lib()

    src_col = meta.columns[0].name
    public_col = public_column_name(src_col)
    data_filename = f"{CHART_SLUG}_tli.csv"

    out_chart_dir = chart_dir(CHART_SLUG)
    write_data_csv(src_csv, out_chart_dir, {src_col: public_col}, data_filename)
    write_html(meta, out_chart_dir, public_col, data_filename)
    print(f"Chart written to {out_chart_dir}")

    print("Uploading to cloud storage...")
    sync_to_storage({
        CHART_LIB_DIR / D3_FILENAME:        f"charts/lib/{D3_FILENAME}",
        CHART_LIB_DIR / PLOT_FILENAME:      f"charts/lib/{PLOT_FILENAME}",
        CHART_LIB_DIR / "longevityplot.js": "charts/lib/longevityplot.js",
        out_chart_dir / data_filename:      f"charts/{CHART_SLUG}/{data_filename}",
        out_chart_dir / "index.html":       f"charts/{CHART_SLUG}/index.html",
    })
    print(f"Chart build complete for {CHART_SLUG}!")


if __name__ == "__main__":
    main()
