import sys
import datetime
import json
import xxhash
from pathlib import Path
from dotenv import find_dotenv

# Use path to determine script intent
script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
SOURCE = script_path.parent.name
CHART_SLUG = DATASET_NAME.replace("_", "-") # Converts 'life_expectancy' to 'life-expectancy'
ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent

print(f"Downloading {DATASET_NAME} from {SOURCE}...")

# Dynamically add the 'scripts/' directory to the Python path
sys.path.append(str(Path(ROOT_PATH, "scripts")))

from utils.owid import download_owid_chart_data
from utils.paths import snapshot_dir, relative_to_data
from utils.storage import sync_to_storage
from schemas import Checksums, CurrentPointer, SnapshotMeta, read_yaml, write_yaml

def main():
    today = datetime.date.today().isoformat()

    # Build local folder architecture
    snap_dir = snapshot_dir(SOURCE, DATASET_NAME)
    version_dir = snap_dir / today

    # If there's already a dataset-TODAY folder, skip
    if version_dir.exists():
        print(f"There's already a {version_dir.name} download! Skipping.")
        sys.exit(0)

    csv_path = version_dir / f"{DATASET_NAME}.csv"
    owid_json_path = version_dir / f"{DATASET_NAME}.owid.json"
    pipeline_yaml_path = version_dir / f"{DATASET_NAME}.meta.yaml"
    current_yaml_path = snap_dir / "current.yaml"

    # Check for previous ETag to only download changed data, and hash to compare downloads in case ETag updates falsely
    previous_etag = None
    previous_hash = None
    if current_yaml_path.exists():
        current = read_yaml(CurrentPointer, current_yaml_path)
        previous_etag = current.etag
        previous_hash = current.csv_hash

    # Download if changed
    print(f"Starting snapshot sync for {SOURCE}/{DATASET_NAME}...")
    result = download_owid_chart_data(CHART_SLUG, previous_etag)

    if result is None:
        print(f"Server returned 304 Not Modified. {DATASET_NAME} is already up to date. Exiting.")
        sys.exit(0)

    csv_bytes, raw_metadata, new_etag = result

    # Calculate checksums before touching the filesystem
    csv_hash = xxhash.xxh3_64_hexdigest(csv_bytes)
    json_bytes = json.dumps(raw_metadata, ensure_ascii=False).encode('utf-8')
    json_hash = xxhash.xxh3_64_hexdigest(json_bytes)

    if csv_hash == previous_hash:
        print(f"Downloaded and existing file hashes match ({csv_hash}). Data is identical. Exiting.")
        sys.exit(0)

    version_dir.mkdir(parents=True, exist_ok=True)

    # Save
    with open(csv_path, "wb") as f:
        f.write(csv_bytes)

    with open(owid_json_path, "wb") as f:
        f.write(json_bytes)

    write_yaml(
        SnapshotMeta(
            dataset=DATASET_NAME,
            source=SOURCE,
            download_date=today,
            original_url=f"https://ourworldindata.org/grapher/{CHART_SLUG}",
            etag=new_etag,
            checksums=Checksums(csv_xxh3_64=csv_hash, json_xxh3_64=json_hash),
        ),
        pipeline_yaml_path,
    )

    write_yaml(
        CurrentPointer(version=today, etag=new_etag, csv_hash=csv_hash),
        current_yaml_path,
    )

    print(f"Artefacts and metadata saved to {version_dir}")

    # Cloud sync
    print("Initiating cloud sync...")
    upload_map = {
        path: relative_to_data(path)
        for path in (csv_path, owid_json_path, pipeline_yaml_path, current_yaml_path)
    }
    sync_to_storage(upload_map)
    print(f"Snapshot pipeline complete for for {SOURCE}/{DATASET_NAME}!")

if __name__ == "__main__":
    main()
