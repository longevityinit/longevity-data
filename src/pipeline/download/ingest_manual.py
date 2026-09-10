"""
Manual ingest helper for data sources that have no public API.

Some sources — notably IHME's Global Burden of Disease (GBD) results tool — can
only be exported by hand through a web UI, and the export is a zip containing a
CSV (sometimes split into several parts) plus a small licence/citation .txt.

This script is the manual-download equivalent of the automated
`download/<source>/<dataset>.py` snapshotters. Because the data isn't fetchable
from anywhere, identity (source/dataset) and provenance (the permalink) come
from CLI arguments rather than the script's path. Everything else — the full
query (every location/age/sex/year present), the checksum, the citation — is
read straight out of the export, so the operator types as little as possible.

The original zip is read in memory and never copied into the repo; only the
canonical extracted artefacts are written. The CSV goes to the snapshot folder
(gitignored) and B2; the licence and metadata YAML are version-controlled.
With --local, all artifacts stay in ignored output/ and nothing is uploaded.

Usage:
    python src/pipeline/download/ingest_manual.py \
        --source ihme --dataset all_cause_deaths \
        --zip ~/Downloads/IHME-GBD_2023_DATA-xxxxx.zip \
        --url "https://vizhub.healthdata.org/gbd-results/?params=gbd-api-2023-public/<hash>"

Optional overrides: --release, --title, --citation, --value-columns, --date.
"""

import argparse
import datetime
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import xxhash
import yaml

ROOT_PATH = Path(__file__).resolve().parents[3]

# Dynamically add the 'src/pipeline/' directory to the Python path
sys.path.append(str(Path(ROOT_PATH, "src/pipeline")))

from utils.paths import get_output_root

# GBD's three estimate columns: the point estimate plus its 95% uncertainty
# interval. Override with --value-columns for other sources.
DEFAULT_VALUE_COLUMNS = ["val", "upper", "lower"]
LICENCE_NAME = "IHME Free-of-Charge Non-commercial User Agreement"


def parse_args():
    p = argparse.ArgumentParser(description="Ingest a manually downloaded data export (zip or csv).")
    p.add_argument("--source", required=True, help="Publisher token, e.g. 'ihme'.")
    p.add_argument("--dataset", required=True, help="Semantic dataset slug, e.g. 'all_cause_deaths'.")
    p.add_argument("--zip", dest="archive", required=True, help="Path to the downloaded zip (or a bare .csv).")
    p.add_argument("--url", required=True, help="Permalink the export was generated from.")
    p.add_argument("--release", help="Data release label, e.g. 'GBD 2023'. Inferred from --url if omitted.")
    p.add_argument("--title", help="Human-readable dataset title. Derived from --dataset if omitted.")
    p.add_argument("--citation", help="Citation string. Read from the bundled licence file or templated if omitted.")
    p.add_argument("--value-columns", help="Comma-separated estimate columns (default: val,upper,lower).")
    p.add_argument("--date", help="Snapshot date YYYY-MM-DD (default: today).")
    p.add_argument("--local", action="store_true", help="Write local artifacts without uploading to storage.")
    return p.parse_args()


def _read_archive(path: Path):
    """
    Returns (csv_bytes, licence_text). Accepts a zip (the normal GBD case) or a
    bare CSV. Multi-part CSVs inside a zip are concatenated, keeping a single
    header, so the snapshot stays one canonical file per dataset.
    """
    if path.suffix.lower() != ".zip":
        return path.read_bytes(), None

    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        csv_names = sorted(n for n in names if n.lower().endswith(".csv"))
        txt_names = [n for n in names if n.lower().endswith(".txt")]

        if not csv_names:
            sys.exit(f"No CSV member found inside {path}. Members: {names}")

        csv_bytes = _merge_csvs(zf, csv_names)

        licence_text = None
        if txt_names:
            # Prefer a citation/terms file if the names hint at one.
            pref = [n for n in txt_names if any(k in n.lower() for k in ("citation", "licen", "terms"))]
            chosen = (pref or txt_names)[0]
            licence_text = zf.read(chosen).decode("utf-8", errors="replace")

    return csv_bytes, licence_text


def _merge_csvs(zf: zipfile.ZipFile, csv_names: list[str]) -> bytes:
    """Byte-level concatenation that preserves the original formatting and drops
    repeated header rows from parts 2..n."""
    parts = []
    first_header = None
    for i, name in enumerate(csv_names):
        data = zf.read(name)
        header, _, body = data.partition(b"\n")
        if i == 0:
            first_header = header
            parts.append(data if data.endswith(b"\n") else data + b"\n")
        else:
            if header.strip() != (first_header or b"").strip():
                print(f"Warning: header of '{name}' differs from the first part; appending anyway.")
            chunk = body
            if chunk and not chunk.endswith(b"\n"):
                chunk += b"\n"
            parts.append(chunk)
    return b"".join(parts)


def _unique(series) -> list:
    return series.dropna().unique().tolist()


def _build_query(df: pd.DataFrame, value_cols: list[str]) -> dict:
    """Reconstruct the full query from the data itself: every distinct value of
    each dimension column. This is what makes the '+205 more' truncation in the
    GBD UI irrelevant, and serves as the discovery index over datasets."""
    dim_cols = [c for c in df.columns if c not in value_cols]
    name_bases = {c[:-len("_name")]: c for c in dim_cols if c.endswith("_name")}

    query: dict[str, list] = {}
    # `*_name` columns are the human-readable dimensions; enumerate those first.
    for base, col in name_bases.items():
        query[base] = sorted(_unique(df[col]))
    # Then any remaining dimensions: bare `*_id`s with no name twin, plus plain
    # columns like `year`.
    for col in dim_cols:
        if col.endswith("_name"):
            continue
        if col.endswith("_id"):
            base = col[:-len("_id")]
            if base in name_bases:
                continue  # already represented by its name column
            query[base] = sorted(_unique(df[col]))
        else:
            query[col] = sorted(_unique(df[col]))
    return query


def _infer_release(url: str, override: str | None) -> tuple[str | None, str | None]:
    """Returns (release_label, year). For GBD permalinks the round is encoded as
    'gbd-api-2023-public'."""
    import re
    m = re.search(r"gbd-api-(\d{4})", url or "")
    year = m.group(1) if m else None
    if override:
        return override, year
    return (f"GBD {year}" if year else None), year


def _resolve_citation(override, licence_text, release, year, url):
    if override:
        return override
    # The bundled licence file usually contains the authoritative citation line.
    if licence_text:
        lines = licence_text.splitlines()
        for i, line in enumerate(lines):
            if "Global Burden of Disease Study" in line:
                block = [line.strip()]
                for nxt in lines[i + 1:]:
                    if not nxt.strip():
                        break
                    block.append(nxt.strip())
                return " ".join(block)
    # Fall back to a templated citation; the operator can refine it later.
    if year:
        return (
            f"Global Burden of Disease Collaborative Network. Global Burden of "
            f"Disease Study {year} ({release}) Results. Seattle, United States: "
            f"Institute for Health Metrics and Evaluation (IHME). "
            f"Available from {url}."
        )
    return None


def main():
    args = parse_args()
    data_path = get_output_root(ROOT_PATH, local=args.local) / "data"
    archive_path = Path(args.archive).expanduser().resolve()
    if not archive_path.exists():
        sys.exit(f"File not found: {archive_path}")

    today = args.date or datetime.date.today().isoformat()
    value_cols = (
        [c.strip() for c in args.value_columns.split(",")]
        if args.value_columns else DEFAULT_VALUE_COLUMNS
    )

    snapshot_dir = Path(data_path, "snapshots") / args.source / args.dataset
    version_dir = snapshot_dir / today
    current_yaml_path = snapshot_dir / "current.yaml"

    if version_dir.exists():
        print(f"There's already a {today} snapshot at {version_dir}! Skipping.")
        sys.exit(0)

    print(f"Reading {archive_path.name}...")
    csv_bytes, licence_text = _read_archive(archive_path)
    csv_hash = xxhash.xxh3_64_hexdigest(csv_bytes)

    # Don't create a new snapshot if the content is byte-identical to the last one.
    if current_yaml_path.exists():
        with open(current_yaml_path, "r", encoding="utf-8") as f:
            previous = yaml.safe_load(f) or {}
        if previous.get("csv_hash") == csv_hash:
            print(f"CSV hash matches the current snapshot ({csv_hash}). Data is identical. Exiting.")
            sys.exit(0)

    df = pd.read_csv(io.BytesIO(csv_bytes))
    query = _build_query(df, [c for c in value_cols if c in df.columns])
    release, year = _infer_release(args.url, args.release)
    citation = _resolve_citation(args.citation, licence_text, release, year, args.url)
    title = args.title or args.dataset.replace("_", " ").capitalize()

    version_dir.mkdir(parents=True, exist_ok=True)
    csv_path = version_dir / f"{args.dataset}.csv"
    meta_path = version_dir / f"{args.dataset}.meta.yaml"
    licence_path = version_dir / f"{args.dataset}.licence.txt"

    with open(csv_path, "wb") as f:
        f.write(csv_bytes)

    meta = {
        "dataset": args.dataset,
        "source": args.source,
        "title": title,
        "download_date": today,
        "download_method": "manual",
        "original_url": args.url,
        "release": release,
        "query": query,
        "checksums": {"csv_xxh3_64": csv_hash},
        "licence": LICENCE_NAME,
        "citation": citation,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(meta, f, sort_keys=False, allow_unicode=True)

    upload_map = {
        csv_path: csv_path.relative_to(data_path).as_posix(),
        meta_path: meta_path.relative_to(data_path).as_posix(),
    }

    if licence_text is not None:
        with open(licence_path, "w", encoding="utf-8") as f:
            f.write(licence_text)
        upload_map[licence_path] = licence_path.relative_to(data_path).as_posix()
    else:
        print("Warning: no licence/citation .txt found in the archive — none saved.")

    with open(current_yaml_path, "w", encoding="utf-8") as f:
        yaml.dump({"version": today, "csv_hash": csv_hash}, f, sort_keys=False)
    upload_map[current_yaml_path] = current_yaml_path.relative_to(data_path).as_posix()

    print(f"Ingested {len(df):,} rows to {version_dir}")

    from utils.storage import sync_to_storage
    sync_to_storage(upload_map, local=args.local)

    print(f"Manual ingest complete for {args.source}/{args.dataset}!")
    print(f"You can now delete the source download: {archive_path}")


if __name__ == "__main__":
    main()
