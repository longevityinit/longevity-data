"""Copy build provenance into Git-visible data/ files for review, without committing."""
import argparse
from pathlib import Path

from utils.provenance import prepare_metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    prepare_metadata(args.manifest.resolve(), Path(__file__).resolve().parents[2])


if __name__ == '__main__':
    main()
