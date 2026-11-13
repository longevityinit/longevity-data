"""Prepare reviewable data snapshots and restore the selected snapshot offline."""
import argparse
import json
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path

import yaml

from download.owid.headline import SOURCES
from pages import ROOT, revision, unpack
from utils.provenance import provenance_files
from utils.publication import checksum, load_snapshot_artifacts, write_json


def prepare(release: str, root: Path = ROOT) -> Path:
    """Bundle the five selected source snapshots and a Git adoption patch."""
    if not release.startswith('graph-data-') or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-' for c in release):
        raise ValueError('Invalid data release tag')
    commit = revision(root)
    output = root / 'output'
    snapshot_root = output / 'data/snapshots/owid'
    files, snapshots = {}, {}
    for dataset in SOURCES:
        pointer = snapshot_root / dataset / 'current.yaml'
        selected = yaml.safe_load(pointer.read_text(encoding='utf-8'))
        version = selected['version']
        if not isinstance(version, str) or not version or any(c not in '0123456789-' for c in version):
            raise ValueError(f'Invalid snapshot version: {dataset}')
        directory = pointer.parent / version
        artifacts, metadata = load_snapshot_artifacts(directory)
        if selected['csv_hash'] != metadata['checksums']['csv_xxh3_64']:
            raise ValueError(f'Pointer checksum mismatch: {dataset}')
        snapshots[dataset] = {'version': version, 'download_date': metadata['download_date']}
        for path in [pointer, *artifacts]:
            files[path.relative_to(output / 'data').as_posix()] = path
    bundle_path = output / 'snapshot.tar.gz'
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        staging = Path(temporary)
        for relative, source in files.items():
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        write_json(staging / 'snapshot.json', snapshots)
        hashes = {p.relative_to(staging).as_posix(): checksum(p)
                  for p in sorted(staging.rglob('*')) if p.is_file()}
        write_json(staging / 'release.json', {'version': 1, 'revision': commit, 'files': hashes})
        with tarfile.open(bundle_path, 'w:gz') as bundle:
            for path in sorted(staging.rglob('*')):
                if path.is_file():
                    bundle.add(path, arcname=path.relative_to(staging).as_posix(), recursive=False)
    selection = {'version': 1, 'release': release, 'sha256': checksum(bundle_path),
                 'source_revision': commit, 'snapshots': snapshots}
    records = provenance_files(output / 'charts/headline-life-expectancy/publish.json')
    with zipfile.ZipFile(output / 'adopt-snapshot.zip', 'w', zipfile.ZIP_DEFLATED) as adoption:
        adoption.writestr('config/pages-snapshot.json', json.dumps(selection, indent=2) + '\n')
        for relative, source in records.items():
            adoption.write(source, relative)
    print(f'Snapshot: {release}\nReview output/graphs.tar.gz and adopt output/adopt-snapshot.zip through a pull request.')
    return bundle_path


def restore(archive: Path, root: Path = ROOT):
    """Restore only the committed selection, checking archive and source checksums."""
    selection_path = root / 'config/pages-snapshot.json'
    if not selection_path.exists():
        raise ValueError('No data snapshot selected. Run Refresh graph data and adopt its review files first.')
    selection = json.loads(selection_path.read_text(encoding='utf-8'))
    if selection.get('version') != 1 or checksum(archive) != selection['sha256']:
        raise ValueError('Selected snapshot archive checksum mismatch')
    output = root / 'output'
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        staging = Path(temporary) / 'snapshot'
        unpack(archive, staging, selection['source_revision'], required_file='snapshot.json', require_deployable=False)
        snapshots = json.loads((staging / 'snapshot.json').read_text(encoding='utf-8'))
        if snapshots != selection['snapshots'] or set(snapshots) != set(SOURCES):
            raise ValueError('Snapshot contents differ from the selected datasets')
        for dataset, record in snapshots.items():
            directory = staging / 'snapshots/owid' / dataset
            pointer = yaml.safe_load((directory / 'current.yaml').read_text(encoding='utf-8'))
            version = record['version']
            if (not isinstance(version, str) or not version
                    or any(c not in '0123456789-' for c in version) or pointer['version'] != version):
                raise ValueError(f'Snapshot pointer mismatch: {dataset}')
            _, metadata = load_snapshot_artifacts(directory / version)
            if pointer['csv_hash'] != metadata['checksums']['csv_xxh3_64']:
                raise ValueError(f'Snapshot pointer checksum mismatch: {dataset}')
        # Replace the selected snapshot directory rather than mixing in old pointers.
        destination = output / 'data/snapshots'
        if destination.exists():
            raise ValueError(f'Snapshot destination already exists: {destination}. Use a fresh output directory.')
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staging / 'snapshots'), destination)


def validate_prepared_run(run: dict, repository: str) -> tuple[str, int]:
    """Only successful main preparation runs may supply a public deployment."""
    if (run.get('path') != '.github/workflows/prepare-graphs.yml'
            or run.get('head_branch') != 'main'
            or run.get('event') not in ('push', 'workflow_dispatch')
            or run.get('status') != 'completed' or run.get('conclusion') != 'success'
            or run.get('head_repository', {}).get('full_name') != repository):
        raise ValueError('Choose a successful Prepare graphs run from main in this repository.')
    sha, attempt = run.get('head_sha', ''), run.get('run_attempt')
    if len(sha) != 40 or any(c not in '0123456789abcdef' for c in sha) or type(attempt) is not int or attempt < 1:
        raise ValueError('Invalid preparation run revision or attempt')
    return sha, attempt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    candidate = commands.add_parser('prepare')
    candidate.add_argument('--release', required=True)
    restore_parser = commands.add_parser('restore')
    restore_parser.add_argument('--archive', type=Path, required=True)
    selected = commands.add_parser('selected-release')
    validate = commands.add_parser('validate-run')
    validate.add_argument('--run', type=Path, required=True)
    validate.add_argument('--repository', required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.release)
    elif args.command == 'restore':
        restore(args.archive)
    elif args.command == 'selected-release':
        path = ROOT / 'config/pages-snapshot.json'
        if not path.exists():
            parser.error('Run Refresh graph data and adopt its review files before preparing graphs.')
        selection = json.loads(path.read_text(encoding='utf-8'))
        release = selection['release']
        if not release.startswith('graph-data-') or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-' for c in release):
            parser.error('Invalid selected snapshot release')
        print(release)
    else:
        sha, attempt = validate_prepared_run(json.loads(args.run.read_text(encoding='utf-8')), args.repository)
        print(f'revision={sha}\nattempt={attempt}')
