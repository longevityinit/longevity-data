import sys
import datetime
import yaml
import xxhash
from pathlib import Path
from dotenv import find_dotenv

script_path = Path(__file__).resolve()
SOURCE = script_path.parent.name  # "hmd"
ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent
DATA_PATH = Path(ROOT_PATH, "data")
CATALOGUE_PATH = script_path.parent / "statistics.yaml"

sys.path.append(str(Path(ROOT_PATH, "scripts")))

from utils.hmd import HMDSession
from utils.storage import sync_to_storage


def load_catalogue() -> tuple[dict, list[dict]]:
    """Returns (site_config, statistics_list) from the YAML catalogue."""
    with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("site", {}), data.get("statistics", [])


def snapshot_agreement(
    session: HMDSession, today: str, agreement_url: str
) -> tuple[dict, list[Path]]:
    """
    Captures the user-agreement page and stores a dated snapshot only when
    the content hash changes. Returns (reference_dict, paths_to_sync).

    The reference dict is embedded in each statistic's meta.yaml so derived
    datasets can prove which agreement version was in force at download time.
    """
    agreement_dir = DATA_PATH / "snapshots" / SOURCE / "_agreement"
    current_path = agreement_dir / "current.yaml"

    text = session.fetch_agreement_text()
    text_bytes = text.encode("utf-8")
    text_hash = xxhash.xxh3_64_hexdigest(text_bytes)

    previous_hash = None
    previous_version = None
    if current_path.exists():
        with open(current_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            previous_hash = data.get("text_hash")
            previous_version = data.get("version")

    ref = {
        "url": agreement_url,
        "text_xxh3_64": text_hash,
    }

    if text_hash == previous_hash:
        ref["version"] = previous_version
        return ref, []

    version_dir = agreement_dir / today
    version_dir.mkdir(parents=True, exist_ok=True)
    text_path = version_dir / "user_agreement.html"
    with open(text_path, "wb") as f:
        f.write(text_bytes)
    with open(current_path, "w", encoding="utf-8") as f:
        yaml.dump({"version": today, "text_hash": text_hash}, f, sort_keys=False)
    print(f"Captured new user-agreement version at {version_dir}")

    ref["version"] = today
    return ref, [text_path, current_path]


def download_statistic(
    session: HMDSession, entry: dict, today: str, agreement_ref: dict
) -> list[Path]:
    """
    Download one statistic if upstream has changed. Returns the list of
    local paths to sync to remote storage (empty if nothing changed).
    """
    name = entry["name"]
    url = entry["url"]

    snapshot_dir = DATA_PATH / "snapshots" / SOURCE / name
    version_dir = snapshot_dir / today
    current_path = snapshot_dir / "current.yaml"

    previous_etag = None
    previous_hash = None
    if current_path.exists():
        with open(current_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            previous_etag = data.get("etag")
            previous_hash = data.get("zip_hash")

    if version_dir.exists():
        print(f"[{name}] {version_dir.name} already exists — skipping.")
        return []

    result = session.download_zip(url, previous_etag)
    if result is None:
        print(f"[{name}] 304 Not Modified — already up to date.")
        return []

    zip_bytes, new_etag = result
    zip_hash = xxhash.xxh3_64_hexdigest(zip_bytes)

    if zip_hash == previous_hash:
        print(f"[{name}] content hash matches previous ({zip_hash}) — skipping.")
        return []

    version_dir.mkdir(parents=True, exist_ok=True)
    zip_path = version_dir / f"{name}.zip"
    meta_path = version_dir / f"{name}.meta.yaml"

    with open(zip_path, "wb") as f:
        f.write(zip_bytes)

    meta = {
        "dataset": name,
        "source": SOURCE,
        "download_date": today,
        "original_url": url,
        "description": entry.get("description"),
        "etag": new_etag,
        "checksums": {"zip_xxh3_64": zip_hash},
        "agreement": agreement_ref,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(meta, f, sort_keys=False)

    with open(current_path, "w", encoding="utf-8") as f:
        yaml.dump(
            {"version": today, "etag": new_etag, "zip_hash": zip_hash},
            f,
            sort_keys=False,
        )

    print(f"[{name}] saved snapshot to {version_dir}")
    return [zip_path, meta_path, current_path]


def main():
    today = datetime.date.today().isoformat()
    site, catalogue = load_catalogue()
    if not catalogue:
        print(f"No statistics listed in {CATALOGUE_PATH}. Nothing to do.")
        return
    required_site_keys = {"base_url", "login_path", "agreement_path"}
    missing = required_site_keys - site.keys()
    if missing:
        raise RuntimeError(
            f"{CATALOGUE_PATH} is missing site keys: {sorted(missing)}"
        )

    session = HMDSession(
        base_url=site["base_url"],
        login_path=site["login_path"],
        agreement_path=site["agreement_path"],
    )
    agreement_url = site["base_url"].rstrip("/") + site["agreement_path"]
    agreement_ref, changed_paths = snapshot_agreement(session, today, agreement_url)

    for entry in catalogue:
        changed_paths.extend(
            download_statistic(session, entry, today, agreement_ref)
        )

    if not changed_paths:
        print("All HMD snapshots up to date. Nothing to upload.")
        return

    print("Initiating cloud sync...")
    upload_map = {p: p.relative_to(DATA_PATH).as_posix() for p in changed_paths}
    sync_to_storage(upload_map)
    print("HMD snapshot pipeline complete.")


if __name__ == "__main__":
    main()
