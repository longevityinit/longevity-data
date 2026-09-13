import argparse
import json
import sys
from pathlib import Path

import yaml

# Use path to determine script intent
script_path = Path(__file__).resolve()
DATASET_NAME = script_path.stem
SOURCE = script_path.parent.name
ROOT_PATH = Path(__file__).resolve().parents[4]


# Dynamically add the 'src/pipeline/' directory to the Python path
sys.path.append(str(Path(ROOT_PATH, "src/pipeline")))

from utils.paths import get_output_root

from utils.owid import standardise_owid_chart_data
from utils.publication import write_manifest, snapshot_manifest


def main():
    parser = argparse.ArgumentParser(description="Run this pipeline stage.")
    parser.parse_args()
    print(f"Standardising {DATASET_NAME} from {SOURCE}...")
    data_path = get_output_root(ROOT_PATH) / "data"
    snapshot_dir = Path(data_path, "snapshots") / SOURCE / DATASET_NAME
    current_yaml_path = snapshot_dir / "current.yaml"

    if not current_yaml_path.exists():
        print(f"No snapshot found at {current_yaml_path}.\n"
              f"Run python src/pipeline/download/{SOURCE}/{DATASET_NAME}.py first.")
        sys.exit(1)

    with open(current_yaml_path, "r", encoding="utf-8") as f:
        current = yaml.safe_load(f) or {}
    version = current.get("version")
    if not version:
        print(f"No 'version' in {current_yaml_path}.")
        sys.exit(1)

    version_dir = snapshot_dir / version
    csv_path = version_dir / f"{DATASET_NAME}.csv"
    json_path = version_dir / f"{DATASET_NAME}.owid.json"

    print(f"Reading snapshot {version} from {version_dir}...")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()
    with open(json_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    source_manifest = snapshot_manifest(version_dir, data_path)

    df, thin_meta = standardise_owid_chart_data(csv_bytes, metadata)

    out_dir = Path(data_path, "standardised") / SOURCE / DATASET_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{DATASET_NAME}.csv"
    out_meta = out_dir / f"{DATASET_NAME}.meta.yaml"

    df.to_csv(out_csv, index=False)

    full_meta = {
        "dataset": DATASET_NAME,
        "source": SOURCE,
        "snapshot_version": version,
        **thin_meta,
    }
    with open(out_meta, "w", encoding="utf-8") as f:
        yaml.dump(full_meta, f, sort_keys=False, allow_unicode=True)

    print(f"Standardised {len(df):,} rows to {out_dir}")

    upload_map = {
        out_csv: out_csv.relative_to(data_path).as_posix(),
        out_meta: out_meta.relative_to(data_path).as_posix(),
    }
    write_manifest(upload_map, out_dir / "publish.json", kind="standardised",
                   dependencies=[source_manifest])
    print(f"Standardise pipeline complete for {SOURCE}/{DATASET_NAME}!")


if __name__ == "__main__":
    main()
