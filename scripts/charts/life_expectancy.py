import sys
import csv
import html
import json
import re
import yaml
import shutil
import requests
import datetime
from pathlib import Path
from dotenv import find_dotenv

script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
CHART_SLUG = DATASET_NAME.replace("_", "-")
ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent
DATA_PATH = ROOT_PATH / "data"
CHARTS_PATH = ROOT_PATH / "charts"
TEMPLATE_FILE = script_path.parent / "templates" / "line_chart.html"

print(f"Building chart for {CHART_SLUG}...")

sys.path.append(str(ROOT_PATH / "scripts"))
from utils.storage import sync_to_storage

PLOT_VERSION = "0.6.16"
PLOT_URL = f"https://cdn.jsdelivr.net/npm/@observablehq/plot@{PLOT_VERSION}/dist/plot.umd.min.js"
VENDOR_FILENAME = f"plot-{PLOT_VERSION}.umd.min.js"
VENDOR_FILE = CHARTS_PATH / "_vendor" / VENDOR_FILENAME


def ensure_vendor():
    if VENDOR_FILE.exists():
        return
    VENDOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Observable Plot {PLOT_VERSION}...")
    r = requests.get(PLOT_URL, timeout=60)
    r.raise_for_status()
    VENDOR_FILE.write_bytes(r.content)
    print(f"Saved {VENDOR_FILE.stat().st_size // 1024} kB to {VENDOR_FILE}")


def public_column_name(name: str) -> str:
    """Strip OWID's per-dataset disambiguation suffix (e.g. life_expectancy_0)."""
    return re.sub(r"_\d+$", "", name)


def write_data_csv(src: Path, dst_dir: Path, rename: dict[str, str]) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "data.csv"
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


def write_html(meta: dict, dst_dir: Path, vendor_rel: str, value_col: str) -> Path:
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    defaults = meta.get("default_selection") or ["World"]
    citation = meta["columns"][0].get("citation_short") or meta.get("citation", "")
    page = (template
            .replace("__CHART_TITLE__", html.escape(meta.get("title", "")))
            .replace("__CITATION__", html.escape(citation))
            .replace("__VENDOR_PATH__", vendor_rel)
            .replace("__VALUE_COL__", value_col)
            .replace("__DEFAULT_SELECTION__", json.dumps(defaults)))
    dst = dst_dir / "index.html"
    dst.write_text(page, encoding="utf-8")
    return dst


def main():
    std_dir = DATA_PATH / "standardised" / "owid" / DATASET_NAME
    src_csv = std_dir / f"{DATASET_NAME}.csv"
    src_meta = std_dir / f"{DATASET_NAME}.meta.yaml"

    if not src_csv.exists():
        print(f"No standardised data at {src_csv}. Run the standardise script first.")
        sys.exit(1)

    with open(src_meta, encoding="utf-8") as f:
        meta = yaml.safe_load(f)

    ensure_vendor()

    src_col = meta["columns"][0]["name"]
    public_col = public_column_name(src_col)
    rename = {src_col: public_col}

    chart_dir = CHARTS_PATH / CHART_SLUG
    write_data_csv(src_csv, chart_dir, rename)
    write_html(meta, chart_dir, f"../_vendor/{VENDOR_FILENAME}", public_col)
    print(f"Chart written to {chart_dir}")

    version = meta.get("snapshot_version", datetime.date.today().isoformat())
    ver_dir = chart_dir / version
    ver_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(chart_dir / "data.csv", ver_dir / "data.csv")
    write_html(meta, ver_dir, f"../../_vendor/{VENDOR_FILENAME}", public_col)
    print(f"Versioned copy at {ver_dir}")

    print("Uploading to cloud storage...")
    sync_to_storage({
        chart_dir / "data.csv": f"charts/{CHART_SLUG}/data.csv",
        chart_dir / "index.html": f"charts/{CHART_SLUG}/index.html",
        ver_dir / "data.csv": f"charts/{CHART_SLUG}/{version}/data.csv",
        ver_dir / "index.html": f"charts/{CHART_SLUG}/{version}/index.html",
    })
    print(f"Chart build complete for {CHART_SLUG}!")


if __name__ == "__main__":
    main()
