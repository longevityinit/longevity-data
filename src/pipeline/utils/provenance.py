"""Prepare and check version-controlled snapshot provenance."""
import json
import subprocess
from pathlib import Path, PurePosixPath

from utils.publication import checksum, validate_manifest


def snapshot_records(entries: list[tuple[Path, dict]]) -> dict[str, Path]:
    """Select only the snapshot metadata, licence, and current pointer."""
    metadata = [entry for _, entry in entries if entry['key'].endswith('.meta.yaml')]
    if len(metadata) != 1:
        raise ValueError('Snapshot must include one metadata file')

    metadata_key = metadata[0]['key']
    parts = PurePosixPath(metadata_key).parts
    if len(parts) != 5 or parts[0] != 'snapshots' or parts[-1] != parts[2] + '.meta.yaml':
        raise ValueError('Invalid snapshot metadata destination')

    version_dir = PurePosixPath(metadata_key).parent
    dataset = parts[2]
    allowed = {
        metadata_key,
        str(version_dir / f'{dataset}.licence.txt'),
        str(version_dir.parent / 'current.yaml'),
    }
    return {'data/' + entry['key']: source for source, entry in entries if entry['key'] in allowed}


def walk_snapshots(path: Path, ancestors: frozenset[Path] = frozenset()):
    """Yield snapshot records after validating each dependency in the build chain."""
    path = path.resolve()
    if path in ancestors:
        raise ValueError('Cyclic publication dependencies')

    entries = validate_manifest(path)
    manifest = json.loads(path.read_text(encoding='utf-8'))
    dependencies = manifest.get('dependencies')
    if not isinstance(dependencies, list):
        raise ValueError('Missing provenance dependencies; rebuild the manifest')

    if manifest.get('kind') == 'snapshot':
        if dependencies:
            raise ValueError('Snapshot manifests cannot have dependencies')
        yield snapshot_records(entries)
        return

    if manifest.get('kind') not in ('chart', 'standardised'):
        raise ValueError('Unknown manifest kind; rebuild it')
    if not dependencies:
        raise ValueError('Build has no source snapshot dependency; rebuild it')

    for dependency in dependencies:
        source = (path.parent / dependency['path']).resolve()
        if checksum(source) != dependency['sha256']:
            raise ValueError(f'Source manifest changed: {source}. Rebuild downstream artifacts.')
        yield from walk_snapshots(source, ancestors | {path})


def provenance_files(manifest_path: Path) -> dict[str, Path]:
    """Collect source provenance, rejecting contradictory records across snapshots."""
    records = {}
    for snapshot in walk_snapshots(manifest_path):
        for target, source in snapshot.items():
            if target in records and records[target].read_bytes() != source.read_bytes():
                raise ValueError(f'Conflicting provenance: {target}')
            records[target] = source
    if not records:
        raise ValueError('No snapshot provenance found')
    return records


def prepare_metadata(manifest_path: Path, repository_root: Path):
    records = provenance_files(manifest_path)
    # Preflight every destination before copying; existing versioned records are immutable.
    for relative, source in records.items():
        target = repository_root / relative
        if not target.resolve().is_relative_to(repository_root.resolve()):
            raise ValueError(f'Provenance destination escapes repository: {target}')
        if target.exists() and target.read_bytes() != source.read_bytes() and target.name != 'current.yaml':
            raise ValueError(f'Conflicting recorded snapshot: {target}. Use a new snapshot version.')
    for relative, source in records.items():
        target = repository_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
        print(f'Prepared {relative}')
    print('Review and commit these files in Git before publishing. Nothing was staged or committed.')


def git_file(repository_root: Path, revision: str, relative: str) -> bytes:
    """Read a tracked file from HEAD or the index (an empty revision)."""
    result = subprocess.run(
        ['git', 'show', f'{revision}:{relative}'],
        cwd=repository_root,
        check=True,
        capture_output=True,
    )
    return result.stdout


def is_committed(source: Path, relative: str, repository_root: Path) -> bool:
    """Require the build, working tree, index, and HEAD to agree."""
    try:
        expected = source.read_bytes()
        working = (repository_root / relative).read_bytes()
        committed = git_file(repository_root, 'HEAD', relative)
        staged = git_file(repository_root, '', relative)
        return expected == working == committed == staged
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def require_committed_provenance(manifest_path: Path, repository_root: Path):
    for relative, source in provenance_files(manifest_path).items():
        if not is_committed(source, relative, repository_root):
            raise ValueError(
                f'Provenance is missing, changed, or uncommitted: {relative}. '
                'Run record_metadata.py --manifest <manifest>, review, and commit before publishing.'
            )
