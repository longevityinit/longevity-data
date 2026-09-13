import argparse
import datetime
import json
import sys
from pathlib import Path

import xxhash
import yaml

# Use path to determine script intent
script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
SOURCE = script_path.parent.name
CHART_SLUG = DATASET_NAME.replace("_", "-") # Converts 'life_expectancy' to 'life-expectancy'
ROOT_PATH = Path(__file__).resolve().parents[4]


# Dynamically add the 'src/pipeline/' directory to the Python path
sys.path.append(str(Path(ROOT_PATH, "src/pipeline")))

from utils.paths import get_output_root

from utils.owid import download_owid_chart_data
from utils.publication import snapshot_manifest


def main():
    parser = argparse.ArgumentParser(description="Run this pipeline stage.")
    parser.parse_args()
    print(f"Downloading {DATASET_NAME} from {SOURCE}...")
    data_path = get_output_root(ROOT_PATH) / "data"
    today = datetime.date.today().isoformat()

    # Build local folder architecture
    snapshot_dir = Path(data_path, "snapshots") / SOURCE / DATASET_NAME
    version_dir = snapshot_dir / today

    # If there's already a dataset-TODAY folder, skip
    if version_dir.exists():
        snapshot_manifest(version_dir, data_path)
        print(f"There's already a {version_dir.name} download! Skipping.")
        sys.exit(0)

    csv_path = version_dir / f"{DATASET_NAME}.csv"
    owid_json_path = version_dir / f"{DATASET_NAME}.owid.json"
    pipeline_yaml_path = version_dir / f"{DATASET_NAME}.meta.yaml"
    current_yaml_path = snapshot_dir / "current.yaml"

    # Check for previous ETag to only download changed data, and hash to compare downloads in case ETag updates falsely
    previous_etag = None
    previous_hash = None
    if current_yaml_path.exists():
        with open(current_yaml_path, "r", encoding="utf-8") as f:
            current_data = yaml.safe_load(f) or {}
            previous_etag = current_data.get("etag")
            previous_hash = current_data.get("csv_hash")

    # Download if changed
    print(f"Starting snapshot sync for {SOURCE}/{DATASET_NAME}...")
    result = download_owid_chart_data(CHART_SLUG, previous_etag)

    if result is None:
        snapshot_manifest(snapshot_dir / current_data["version"], data_path)
        print(f"Server returned 304 Not Modified. {DATASET_NAME} is already up to date. Exiting.")
        sys.exit(0)

    csv_bytes, raw_metadata, new_etag = result

    # Calculate checksums before touching the filesystem
    csv_hash = xxhash.xxh3_64_hexdigest(csv_bytes)
    json_bytes = json.dumps(raw_metadata, ensure_ascii=False).encode('utf-8')
    json_hash = xxhash.xxh3_64_hexdigest(json_bytes)

    if csv_hash == previous_hash:
        snapshot_manifest(snapshot_dir / current_data["version"], data_path)
        print(f"Downloaded and existing file hashes match ({csv_hash}). Data is identical. Exiting.")
        sys.exit(0)

    version_dir.mkdir(parents=True, exist_ok=True)

    # Save
    with open(csv_path, "wb") as f:
        f.write(csv_bytes)

    with open(owid_json_path, "wb") as f:
        f.write(json_bytes)

    pipeline_meta = {
        "dataset": DATASET_NAME,
        "source": SOURCE,
        "download_date": today,
        "original_url": f"https://ourworldindata.org/grapher/{CHART_SLUG}",
        "etag": new_etag,
        "checksums": {
            "csv_xxh3_64": csv_hash,
            "json_xxh3_64": json_hash
        }
    }
    with open(pipeline_yaml_path, "w", encoding="utf-8", newline="\n") as f:
        yaml.dump(pipeline_meta, f, sort_keys=False)

    with open(current_yaml_path, "w", encoding="utf-8", newline="\n") as f:
        yaml.dump({"version": today, "etag": new_etag, "csv_hash": csv_hash}, f, sort_keys=False)

    print(f"Artefacts and metadata saved to {version_dir}")

    snapshot_manifest(version_dir, data_path)
    print(f"Snapshot pipeline complete for {SOURCE}/{DATASET_NAME}!")


if __name__ == "__main__":
    main()
