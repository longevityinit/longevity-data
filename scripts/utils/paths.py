"""Project path helpers.

Single source of truth for filesystem layout. Every other script should derive
its paths from these helpers rather than rebuilding them, so a change to the
on-disk structure is a one-file edit.
"""
from pathlib import Path
from dotenv import find_dotenv

ROOT_PATH = Path(find_dotenv(raise_error_if_not_found=True)).parent
DATA_PATH = ROOT_PATH / "data"
CHARTS_PATH = ROOT_PATH / "charts"
SCRIPTS_PATH = ROOT_PATH / "scripts"
CHART_LIB_DIR = CHARTS_PATH / "lib"


def snapshot_dir(source: str, dataset: str) -> Path:
    return DATA_PATH / "snapshots" / source / dataset


def standardised_dir(source: str, dataset: str) -> Path:
    return DATA_PATH / "standardised" / source / dataset


def chart_dir(slug: str) -> Path:
    return CHARTS_PATH / slug


def relative_to_data(path: Path) -> str:
    """Storage key for a file under data/, suitable for the cloud bucket."""
    return path.relative_to(DATA_PATH).as_posix()
