"""Summarize data changes and apply only candidate adoption records for a PR."""
import argparse
import json
import math
import zipfile
from pathlib import Path

from download.owid.headline import SOURCES, ALL_SOURCES
from pages import ROOT


def apply_adoption(archive: Path, root: Path = ROOT):
    """Preflight the entire archive before changing selection/provenance files."""
    with zipfile.ZipFile(archive) as bundle:
        entries = bundle.infolist()
        names = [entry.filename for entry in entries]
        if (len(names) != len(set(names)) or 'config/pages-snapshot.json' not in names
                or len(names) > 24 or sum(entry.file_size for entry in entries) > 2_000_000):
            raise ValueError('Invalid adoption file list or size')
        selection = json.loads(bundle.read('config/pages-snapshot.json'))
        snapshots = selection.get('snapshots', {})
        if selection.get('version') != 1 or set(snapshots) not in (set(SOURCES), set(ALL_SOURCES)):
            raise ValueError('Invalid adoption selection')
        required = {'config/pages-snapshot.json'}
        optional = set()
        for dataset, record in snapshots.items():
            version = record['version']
            if not isinstance(version, str) or not version or any(c not in '0123456789-' for c in version):
                raise ValueError(f'Invalid candidate version: {dataset}')
            directory = f'data/snapshots/owid/{dataset}'
            required.update((f'{directory}/current.yaml', f'{directory}/{version}/{dataset}.meta.yaml'))
            optional.add(f'{directory}/{version}/{dataset}.licence.txt')
        if not required.issubset(names) or set(names) - required - optional:
            raise ValueError('Adoption archive contains missing or unrelated files')
        prepared = []
        for name in names:
            target = root / name
            if not target.resolve().is_relative_to(root.resolve()):
                raise ValueError(f'Adoption target escapes checkout: {name}')
            content = bundle.read(name)
            if target.exists() and target.name not in ('current.yaml', 'pages-snapshot.json') and target.read_bytes() != content:
                raise ValueError(f'Conflicting immutable provenance: {name}')
            prepared.append((target, content))
        for target, content in prepared:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)


def summarize(previous: dict | None, candidate: dict) -> str:
    """Describe changed values/coverage; source compatibility still needs review."""
    def index(data):
        result = {}
        for row in data['rows']:
            key = row['sex'], row['year'], row['series']
            if key in result or (row['value'] is not None and not math.isfinite(row['value'])):
                raise ValueError(f'Duplicate or invalid graph observation: {key}')
            result[key] = row['value']
        return result
    new = index(candidate)
    lines = ['## Data review', '',
             f"Candidate coverage: {candidate['start_year']}–{candidate['end_year']}; {len(new):,} rows."]
    if previous is None:
        lines += ['', 'No previous selected snapshot is available for comparison.']
    else:
        old = index(previous)
        shared = old.keys() & new.keys()
        changed = [key for key in shared if old[key] != new[key]]
        numeric = [abs(new[key] - old[key]) for key in changed if new[key] is not None and old[key] is not None]
        became_missing = sum(old[key] is not None and new[key] is None for key in shared)
        filled = sum(old[key] is None and new[key] is not None for key in shared)
        lines += ['', f'- Changed values: **{len(changed):,}** of {len(shared):,} shared rows.',
                  f'- Added rows: **{len(new.keys() - old.keys()):,}**; removed rows: **{len(old.keys() - new.keys()):,}**.',
                  f'- Newly missing values: **{became_missing:,}**; newly available values: **{filled:,}**.']
        if numeric:
            lines.append(f'- Largest absolute value change: **{max(numeric):.6g} years**.')
    lines += ['', 'Review source metadata and methodology as well as arithmetic. This summary does not resolve historical geography or source-definition questions.',
              '', 'Merging this PR selects the candidate data and triggers Prepare graphs. It does not deploy the site.', '']
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    adoption = commands.add_parser('apply')
    adoption.add_argument('--archive', type=Path, required=True)
    report = commands.add_parser('report')
    report.add_argument('--previous', type=Path)
    report.add_argument('--candidate', type=Path, required=True)
    report.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'apply':
        apply_adoption(args.archive)
    else:
        previous = json.loads(args.previous.read_text(encoding='utf-8')) if args.previous and args.previous.exists() else None
        candidate = json.loads(args.candidate.read_text(encoding='utf-8'))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(summarize(previous, candidate), encoding='utf-8')
