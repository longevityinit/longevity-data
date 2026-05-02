import sys
import csv
import html
import json
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

print(f"Building chart for {CHART_SLUG}...")

sys.path.append(str(ROOT_PATH / "scripts"))
from utils.storage import sync_to_storage

PLOT_VERSION = "0.6.16"
PLOT_URL = f"https://cdn.jsdelivr.net/npm/@observablehq/plot@{PLOT_VERSION}/dist/plot.umd.min.js"
VENDOR_FILE = CHARTS_PATH / "_vendor" / "plot.umd.min.js"

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__CHART_TITLE__</title>
  <script src="__VENDOR_PATH__"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, -apple-system, sans-serif; background: #fff; padding: 1rem; }
    #chart svg { width: 100%; height: auto; }
    #source { margin-top: 0.5rem; font-size: 0.75rem; color: #888; }
    [aria-label="line"] path { transition: opacity 0.15s ease; }
    .fade-others [aria-label="line"] path { opacity: 0.15; }
    .fade-others [aria-label="line"] path.hovered { opacity: 1; }
  </style>
</head>
<body>
  <div id="chart"></div>
  <p id="source">Source: __CITATION__</p>
  <script type="module">
    function parseCSV(text) {
      function splitLine(line) {
        const fields = [];
        let field = '', quoted = false;
        for (const ch of line) {
          if (ch === '"') { quoted = !quoted; }
          else if (ch === ',' && !quoted) { fields.push(field); field = ''; }
          else { field += ch; }
        }
        fields.push(field);
        return fields;
      }
      const lines = text.trim().split('\\n');
      const headers = splitLine(lines[0]);
      return lines.slice(1)
        .filter(l => l.trim())
        .map(l => {
          const vals = splitLine(l);
          return Object.fromEntries(headers.map((h, i) => [h, vals[i] ?? '']));
        });
    }

    const params = new URLSearchParams(location.hash.slice(1));
    const filter = params.get('location')?.split(',') ?? null;
    const defaults = __DEFAULT_SELECTION__;

    const resp = await fetch(new URL('data.csv', import.meta.url));
    const allRows = parseCSV(await resp.text());

    const visible = filter
      ? allRows.filter(d => filter.includes(d.code) || filter.includes(d.entity))
      : allRows.filter(d => defaults.includes(d.entity) || defaults.includes(d.code));

    const data = visible
      .filter(d => d.__VALUE_COL__ !== '' && d.__VALUE_COL__ != null)
      .map(d => ({ entity: d.entity, code: d.code, year: +d.year, value: +d.__VALUE_COL__ }));

    const chart = Plot.plot({
      style: { fontSize: '13px' },
      marginLeft: 48,
      x: { label: null },
      y: { label: 'Years', grid: true },
      color: { legend: true },
      marks: [
        Plot.lineY(data, {
          x: 'year', y: 'value', stroke: 'entity',
          strokeWidth: 2, curve: 'monotone-x',
        }),
        Plot.tip(data, Plot.pointerX({
          x: 'year', y: 'value',
          title: d => `${d.entity}: ${d.value} years (${d.year})`,
        })),
      ],
    });

    document.getElementById('chart').append(chart);

    const svg = chart.tagName === 'svg' ? chart : chart.querySelector('svg');
    const paths = [...svg.querySelectorAll('[aria-label="line"] path')];

    paths.forEach(path => {
      path.style.pointerEvents = 'visibleStroke';
      path.addEventListener('mouseenter', () => {
        svg.classList.add('fade-others');
        path.classList.add('hovered');
      });
      path.addEventListener('mouseleave', () => {
        svg.classList.remove('fade-others');
        path.classList.remove('hovered');
      });
    });
  </script>
</body>
</html>
"""


def ensure_vendor():
    if VENDOR_FILE.exists():
        return
    VENDOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Observable Plot {PLOT_VERSION}...")
    r = requests.get(PLOT_URL, timeout=60)
    r.raise_for_status()
    VENDOR_FILE.write_bytes(r.content)
    print(f"Saved {VENDOR_FILE.stat().st_size // 1024} kB to {VENDOR_FILE}")


def write_data_csv(src: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "data.csv"
    rows = []
    with open(src, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for k in row:
                if k not in ("entity", "code", "year"):
                    try:
                        row[k] = f"{float(row[k]):.2f}" if row[k] else ""
                    except ValueError:
                        pass
            rows.append(row)
    with open(dst, "w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    return dst


def write_html(meta: dict, dst_dir: Path, vendor_rel: str) -> Path:
    col = meta["columns"][0]["name"]
    defaults = meta.get("default_selection") or ["World"]
    citation = meta["columns"][0].get("citation_short") or meta.get("citation", "")
    page = (HTML_TEMPLATE
            .replace("__CHART_TITLE__", html.escape(meta.get("title", "")))
            .replace("__CITATION__", html.escape(citation))
            .replace("__VENDOR_PATH__", vendor_rel)
            .replace("__VALUE_COL__", col)
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

    chart_dir = CHARTS_PATH / CHART_SLUG
    write_data_csv(src_csv, chart_dir)
    write_html(meta, chart_dir, "../_vendor/plot.umd.min.js")
    print(f"Chart written to {chart_dir}")

    version = meta.get("snapshot_version", datetime.date.today().isoformat())
    ver_dir = chart_dir / version
    ver_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(chart_dir / "data.csv", ver_dir / "data.csv")
    write_html(meta, ver_dir, "../../_vendor/plot.umd.min.js")
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
