"""Build publication manifests without accessing remote storage."""
import hashlib
import json
import mimetypes
import os
from pathlib import Path, PurePosixPath

import xxhash
import yaml


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8', newline='\n')
    temporary.replace(path)


def write_manifest(
    upload_map: dict[Path, str],
    path: Path,
    *,
    finalize: Path | None = None,
    kind: str = 'snapshot',
    dependencies: list[Path] | None = None,
):
    """Record files and destinations, freezing the snapshot pointer if present."""
    files = []
    for source, key in upload_map.items():
        is_final = source == finalize
        if is_final:
            frozen = path.parent / '.publish' / source.name
            frozen.parent.mkdir(parents=True, exist_ok=True)
            frozen.write_bytes(source.read_bytes())
            source = frozen
        # Git stores provenance text with LF; normalize before recording byte hashes.
        # Source data (CSV and upstream JSON) must retain its original bytes.
        if kind == 'snapshot' and key.endswith(('.meta.yaml', '.licence.txt', '/current.yaml')):
            original = source.read_bytes()
            normalized = original.replace(b'\r\n', b'\n')
            if normalized != original:
                source.write_bytes(normalized)
        content_type = mimetypes.guess_type(key)[0] or 'application/octet-stream'
        files.append({
            'path': os.path.relpath(source, path.parent),
            'key': key,
            'sha256': checksum(source),
            'size': source.stat().st_size,
            'content_type': content_type,
            'finalize': is_final,
        })
    refs = [{'path': os.path.relpath(dep, path.parent), 'sha256': checksum(dep)}
            for dep in (dependencies or [])]
    write_json(path, {'version': 2, 'kind': kind, 'files': files, 'dependencies': refs})
    print(f'Publication manifest: {path}')


def validate_manifest(path: Path) -> list[tuple[Path, dict]]:
    manifest = json.loads(path.read_text(encoding='utf-8'))
    if manifest.get('version') != 2 or not isinstance(manifest.get('files'), list) or not manifest['files']:
        raise ValueError('Unsupported or empty publication manifest')
    entries, keys = [], set()
    for entry in manifest['files']:
        key = entry['key']
        if (
            not isinstance(key, str) or not key or key.startswith('/')
            or '..' in PurePosixPath(key).parts or key in keys
        ):
            raise ValueError(f'Invalid or duplicate destination: {key}')
        keys.add(key)
        source = (path.parent / entry['path']).resolve()
        if (
            not source.is_file()
            or source.stat().st_size != entry['size']
            or checksum(source) != entry['sha256']
        ):
            raise ValueError(f'Missing or changed artifact: {source}. Rebuild its manifest before publishing.')
        if not isinstance(entry.get('finalize'), bool) or not isinstance(entry.get('content_type'), str):
            raise ValueError(f'Invalid publication entry: {key}')
        entries.append((source, entry))
    return sorted(entries, key=lambda item: item[1]['finalize'])


def load_snapshot_artifacts(version_dir: Path) -> tuple[list[Path], dict]:
    """Load snapshot metadata and check the artifacts against its checksums."""
    dataset = version_dir.parent.name
    metadata_path = version_dir / f'{dataset}.meta.yaml'
    metadata = yaml.safe_load(metadata_path.read_text(encoding='utf-8'))
    csv_path = version_dir / f'{dataset}.csv'
    if xxhash.xxh3_64_hexdigest(csv_path.read_bytes()) != metadata['checksums']['csv_xxh3_64']:
        raise ValueError(f'Snapshot checksum mismatch: {csv_path}')
    artifacts = [csv_path, metadata_path]
    if metadata.get('source') == 'owid' and metadata.get('download_method') != 'manual':
        raw_metadata = version_dir / f'{dataset}.owid.json'
        if xxhash.xxh3_64_hexdigest(raw_metadata.read_bytes()) != metadata['checksums']['json_xxh3_64']:
            raise ValueError(f'Snapshot checksum mismatch: {raw_metadata}')
        artifacts.insert(1, raw_metadata)
    licence = version_dir / f'{dataset}.licence.txt'
    if licence.exists():
        artifacts.append(licence)
    # Verify artifacts before writing anything, including interrupted older builds.
    for artifact in artifacts:
        if not artifact.is_file():
            raise ValueError(f'Incomplete snapshot: {artifact}')
    return artifacts, metadata


def write_snapshot_pointer(version_dir: Path, metadata: dict) -> Path:
    """Freeze this version's pointer independently of the latest local snapshot."""
    pointer = version_dir / '.publish/current.yaml'
    pointer.parent.mkdir(exist_ok=True)
    current = {'version': version_dir.name, 'csv_hash': metadata['checksums']['csv_xxh3_64']}
    if 'etag' in metadata:
        current['etag'] = metadata['etag']
    pointer.write_text(yaml.safe_dump(current, sort_keys=False), encoding='utf-8', newline='\n')
    return pointer


def snapshot_manifest(version_dir: Path, data_root: Path) -> Path:
    """Prepare publication for a completed snapshot, including older local builds."""
    artifacts, metadata = load_snapshot_artifacts(version_dir)
    pointer = write_snapshot_pointer(version_dir, metadata)
    upload_map = {artifact: artifact.relative_to(data_root).as_posix() for artifact in artifacts}
    upload_map[pointer] = (version_dir.parent / 'current.yaml').relative_to(data_root).as_posix()
    path = version_dir / 'publish.json'
    write_manifest(upload_map, path, finalize=pointer)
    return path
