"""Snapshot the OWID export used by the headline graph."""
import datetime
import json
import sys
from pathlib import Path

import xxhash
import yaml

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from utils.owid import download_owid_chart_data
from utils.publication import snapshot_manifest

SOURCES = {
    'headline': 'life-expectancy-of-women-vs-life-expectancy-of-men',
    'headline_history': 'life-expectation-at-birth-by-sex',
    'headline_population': 'population',
    'headline_population_female': 'female-population-by-age-group',
    'headline_population_male': 'male-population-by-age-group',
}


def download(dataset, slug):
    data = ROOT / 'output/data'
    directory = data / 'snapshots/owid' / dataset
    version = datetime.date.today().isoformat()
    target = directory / version
    if target.exists():
        snapshot_manifest(target, data)
        print(f'Using existing snapshot {version}')
        return
    csv_bytes, metadata, etag = download_owid_chart_data(slug)
    json_bytes = json.dumps(metadata, ensure_ascii=False).encode('utf-8')
    record = {
        'dataset': dataset, 'source': 'owid', 'download_date': version,
        'original_url': f'https://ourworldindata.org/grapher/{slug}', 'etag': etag,
        'checksums': {'csv_xxh3_64': xxhash.xxh3_64_hexdigest(csv_bytes),
                      'json_xxh3_64': xxhash.xxh3_64_hexdigest(json_bytes)},
    }
    target.mkdir(parents=True)
    (target / f'{dataset}.csv').write_bytes(csv_bytes)
    (target / f'{dataset}.owid.json').write_bytes(json_bytes)
    (target / f'{dataset}.meta.yaml').write_text(yaml.safe_dump(record, sort_keys=False), encoding='utf-8')
    (directory / 'current.yaml').write_text(yaml.safe_dump({
        'version': version, 'etag': etag, 'csv_hash': record['checksums']['csv_xxh3_64'],
    }), encoding='utf-8')
    snapshot_manifest(target, data)


if __name__ == '__main__':
    for dataset, slug in SOURCES.items():
        download(dataset, slug)
