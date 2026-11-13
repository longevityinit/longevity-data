"""Package verified charts for GitHub Pages, without downloading or publishing."""
import argparse
import html
import hashlib
import json
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

from charts.headline import source_assets
from utils.provenance import git_file, require_committed_provenance
from utils.publication import checksum, validate_manifest, write_json

ROOT = Path(__file__).resolve().parents[2]
TITLES = {
    'headline-life-expectancy': ('Life expectancy at birth', 'Compare countries, best practice, and population-weighted percentiles.'),
}


def revision(root: Path) -> str:
    status = subprocess.run(['git', 'status', '--porcelain'], cwd=root, check=True,
                            capture_output=True, text=True).stdout
    if status:
        raise ValueError('Commit or remove checkout changes before packaging a public release.')
    return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, check=True,
                          capture_output=True, text=True).stdout.strip()


def build(manifests: list[Path], root: Path = ROOT, *, candidate: bool = False) -> Path:
    """Copy only manifest-listed public assets, retaining their relative layout."""
    commit = revision(root)
    charts = (root / 'output/charts').resolve()
    files = {}
    selected = []
    presentation = {}
    for manifest in manifests:
        manifest = manifest.resolve()
        slug = manifest.parent.name
        if slug not in TITLES or slug in selected or manifest.parent.parent != charts:
            raise ValueError(f'Unsupported or duplicate chart manifest: {manifest}')
        data = json.loads(manifest.read_text(encoding='utf-8'))
        if data.get('kind') != 'chart':
            raise ValueError(f'Expected a chart manifest: {manifest}')
        if candidate:
            # Still validate the complete input chain; only Git adoption is pending.
            from utils.provenance import provenance_files
            provenance_files(manifest)
        else:
            require_committed_provenance(manifest, root)
        entries = validate_manifest(manifest)
        for source, entry in entries:
            if not source.is_relative_to(charts):
                raise ValueError(f'Chart asset escapes chart directory: {source}')
            relative = source.relative_to(charts).as_posix()
            if entry['key'] != 'charts/' + relative:
                raise ValueError(f'Unexpected chart destination: {entry["key"]}')
            if relative in files and files[relative] != source:
                raise ValueError(f'Conflicting asset: {relative}')
            files[relative] = source
        if slug + '/index.html' not in files:
            raise ValueError(f'Missing chart entry point: {slug}')
        if slug == 'headline-life-expectancy':
            chart_data = json.loads((charts / slug / 'headline.json').read_text(encoding='utf-8'))
            if chart_data.get('code_revision') != commit or chart_data.get('code_dirty') is not False:
                raise ValueError('Rebuild headline indicators from the clean release commit.')
        # Rebuild presentation files from this commit instead of trusting a previous
        # chart build's manifest: its hashes establish integrity, not code revision.
        for name, source in source_assets(root).items():
            relative = source.relative_to(root).as_posix()
            committed = git_file(root, commit, relative)
            if source.read_bytes() != committed:
                raise ValueError(f'Chart source differs from release commit: {relative}')
            destination = slug + '/' + name
            if destination not in files:
                raise ValueError(f'Chart manifest is missing presentation asset: {destination}')
            presentation[destination] = committed
        selected.append(slug)
    if not selected:
        raise ValueError('Select at least one chart manifest.')
    output = root / 'output'
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        staging = Path(temporary) / 'site'
        staging.mkdir()
        for relative, source in files.items():
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        for relative, content in presentation.items():
            (staging / relative).write_bytes(content)
        cards = ''.join(
            f'<li><a href="{slug}/">{html.escape(TITLES[slug][0])}</a>'
            f'<p>{html.escape(TITLES[slug][1])}</p></li>'
            for slug in selected)
        template = (root / 'src/site/index.html').read_text(encoding='utf-8')
        (staging / 'index.html').write_text(template.replace('__GRAPHS__', cards), encoding='utf-8')
        shutil.copyfile(root / 'src/site/site.css', staging / 'site.css')
        shutil.copyfile(root / 'LICENSE', staging / 'LICENSE')
        (staging / '.nojekyll').touch()
        selection_path = root / 'config/pages-snapshot.json'
        snapshot = json.loads(selection_path.read_text(encoding='utf-8')) if selection_path.exists() and not candidate else None
        if snapshot and not candidate:
            dates = ', '.join(sorted({record['download_date'] for record in snapshot['snapshots'].values()}))
            marker = '<p class="build-info">' + html.escape('Data: ' + dates + ' · Snapshot: ' + snapshot['release'] + ' · Code: ' + commit[:8]) + '</p>'
            homepage = staging / 'index.html'
            homepage.write_text(homepage.read_text(encoding='utf-8').replace('</main>', marker + '</main>'), encoding='utf-8')
        hashes = {p.relative_to(staging).as_posix(): checksum(p)
                  for p in sorted(staging.rglob('*')) if p.is_file()}
        write_json(staging / 'release.json', {'version': 1, 'revision': commit, 'deployable': not candidate, 'snapshot': snapshot, 'files': hashes})
        destination = output / 'pages'
        if destination.exists():
            shutil.rmtree(destination)
        staging.rename(destination)
    archive = output / 'graphs.tar.gz'
    with tarfile.open(archive, 'w:gz') as bundle:
        for path in sorted(destination.rglob('*')):
            if path.is_file():
                bundle.add(path, arcname=path.relative_to(destination).as_posix(), recursive=False)
    print(f'Pages bundle: {archive}\nSHA-256: {checksum(archive)}\nSource revision: {commit}')
    return archive


def unpack(archive: Path, destination: Path, expected_revision: str, *,
           required_file: str = 'index.html', require_deployable: bool = True):
    """Validate a release before extraction; reject links, extra files and traversal."""
    with tarfile.open(archive, 'r:gz') as bundle:
        members = bundle.getmembers()
        names = set()
        if len(members) > 1000 or sum(m.size for m in members) > 1024 ** 3:
            raise ValueError('Release exceeds Pages bundle limits.')
        for member in members:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or '..' in path.parts
                    or member.name != path.as_posix() or member.name in names):
                raise ValueError(f'Unsafe or duplicate archive entry: {member.name}')
            names.add(member.name)
        if 'release.json' not in names:
            raise ValueError('Missing release manifest.')
        release = json.load(bundle.extractfile('release.json'))
        if release.get('version') != 1 or release.get('revision') != expected_revision:
            raise ValueError('Release does not match the selected source revision.')
        if require_deployable and release.get('deployable') is not True:
            raise ValueError('Candidate snapshots cannot be deployed; adopt the data and prepare on main.')
        files = release.get('files')
        if not isinstance(files, dict) or set(files) | {'release.json'} != names or required_file not in files:
            raise ValueError('Release file list does not match archive.')
        for name, digest in files.items():
            stream = bundle.extractfile(name)
            actual = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                actual.update(chunk)
            if actual.hexdigest() != digest:
                raise ValueError(f'Release checksum mismatch: {name}')
        if destination.exists():
            raise ValueError(f'Destination already exists: {destination}')
        destination.mkdir(parents=True)
        for member in members:
            target = destination / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as source, target.open('wb') as target_file:
                shutil.copyfileobj(source, target_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('build')
    prepare.add_argument('--candidate', action='store_true', help='Create a non-deployable data review bundle')
    prepare.add_argument('--manifest', type=Path, action='append', required=True)
    restore = commands.add_parser('unpack')
    restore.add_argument('--archive', type=Path, required=True)
    restore.add_argument('--destination', type=Path, required=True)
    restore.add_argument('--revision', required=True)
    args = parser.parse_args()
    if args.command == 'build':
        build(args.manifest, candidate=args.candidate)
    else:
        unpack(args.archive, args.destination, args.revision)
