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
from utils.paths import snapshot_dir, standardised_dir, relative_to_data
from utils.storage import sync_to_storage
from schemas import CurrentPointer, StandardisedMeta, read_yaml, write_yaml

def main():
    snap_dir = snapshot_dir(SOURCE, DATASET_NAME)
    current_yaml_path = snap_dir / "current.yaml"

    if not current_yaml_path.exists():
        print(f"No snapshot found at {current_yaml_path}. Run the download script first.")
        sys.exit(1)

    current = read_yaml(CurrentPointer, current_yaml_path)
    version = current.version

    version_dir = snap_dir / version
    csv_path = version_dir / f"{DATASET_NAME}.csv"
    json_path = version_dir / f"{DATASET_NAME}.owid.json"

    print(f"Reading snapshot {version} from {version_dir}...")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()
    with open(json_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    df, thin_meta = standardise_owid_chart_data(csv_bytes, metadata)

    out_dir = standardised_dir(SOURCE, DATASET_NAME)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{DATASET_NAME}.csv"
    out_meta_path = out_dir / f"{DATASET_NAME}.meta.yaml"

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
    upload_map = {path: relative_to_data(path) for path in (out_csv, out_meta_path)}
    sync_to_storage(upload_map)
    print(f"Standardise pipeline complete for {SOURCE}/{DATASET_NAME}!")

if __name__ == "__main__":
    main()
