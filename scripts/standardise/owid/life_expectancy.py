import sys
import json
from pathlib import Path
from dotenv import find_dotenv

# Use path to determine script intent
script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
SOURCE = script_path.parent.name
ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent

print(f"Standardising {DATASET_NAME} from {SOURCE}...")

# Dynamically add the 'scripts/' directory to the Python path
sys.path.append(str(Path(ROOT_PATH, "scripts")))

from utils.owid import standardise_owid_chart_data
from utils.paths import DATA_PATH, snapshot_dir, standardised_dir
from utils.storage import sync_to_storage, upload_map
from schemas import CurrentPointer, StandardisedMeta, read_yaml, write_yaml
from config import (
    CURRENT_POINTER_FILENAME,
    DATA_CSV_EXT,
    META_YAML_EXT,
    OWID_RAW_META_EXT,
)

def main():
    snap_dir = snapshot_dir(SOURCE, DATASET_NAME)
    current_yaml_path = snap_dir / CURRENT_POINTER_FILENAME

    if not current_yaml_path.exists():
        print(f"No snapshot found at {current_yaml_path}. Run the download script first.")
        sys.exit(1)

    current = read_yaml(CurrentPointer, current_yaml_path)
    version = current.version

    version_dir = snap_dir / version
    csv_path = version_dir / f"{DATASET_NAME}{DATA_CSV_EXT}"
    json_path = version_dir / f"{DATASET_NAME}{OWID_RAW_META_EXT}"

    print(f"Reading snapshot {version} from {version_dir}...")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()
    with open(json_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    df, thin_meta = standardise_owid_chart_data(csv_bytes, metadata)

    out_dir = standardised_dir(SOURCE, DATASET_NAME)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{DATASET_NAME}{DATA_CSV_EXT}"
    out_meta_path = out_dir / f"{DATASET_NAME}{META_YAML_EXT}"

    df.to_csv(out_csv, index=False)

    out_meta = StandardisedMeta(
        dataset=DATASET_NAME,
        source=SOURCE,
        snapshot_version=version,
        **thin_meta,
    )
    write_yaml(out_meta, out_meta_path)

    print(f"Standardised {len(df):,} rows to {out_dir}")

    print("Initiating cloud sync...")
    sync_to_storage(upload_map(DATA_PATH, out_csv, out_meta_path))
    print(f"Standardise pipeline complete for {SOURCE}/{DATASET_NAME}!")

if __name__ == "__main__":
    main()
