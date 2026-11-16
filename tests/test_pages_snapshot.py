"""Selected snapshots remain fixed, verifiable and independent of site releases."""
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import xxhash
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
import pages_snapshot
from utils.publication import snapshot_manifest, write_manifest


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'producer'
        self.root.mkdir()
        (self.root / '.gitignore').write_text('/output/\n', encoding='utf-8')
        for args in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.org', 'commit', '-qm', 'fixture']):
            subprocess.run(['git', *args], cwd=self.root, check=True, capture_output=True)
        dependencies = []
        for dataset in pages_snapshot.SOURCES:
            directory = self.root / 'output/data/snapshots/owid' / dataset
            version = directory / '2026-11-13-123-1'
            version.mkdir(parents=True)
            csv = b'Year,Value\n2000,70\n'
            raw = b'{"chart": {}}'
            (version / f'{dataset}.csv').write_bytes(csv)
            (version / f'{dataset}.owid.json').write_bytes(raw)
            metadata = {'source': 'owid', 'download_date': '2026-11-13', 'checksums': {
                'csv_xxh3_64': xxhash.xxh3_64_hexdigest(csv),
                'json_xxh3_64': xxhash.xxh3_64_hexdigest(raw)}}
            (version / f'{dataset}.meta.yaml').write_text(yaml.safe_dump(metadata), encoding='utf-8')
            (directory / 'current.yaml').write_text(yaml.safe_dump({
                'version': version.name, 'csv_hash': metadata['checksums']['csv_xxh3_64']}), encoding='utf-8')
            dependencies.append(snapshot_manifest(version, self.root / 'output/data'))
        chart = self.root / 'output/charts/headline-life-expectancy'
        chart.mkdir(parents=True)
        (chart / 'index.html').write_text('fixture', encoding='utf-8')
        write_manifest({chart / 'index.html': 'charts/headline-life-expectancy/index.html'},
                       chart / 'publish.json', kind='chart', dependencies=dependencies)
        self.archive = pages_snapshot.prepare('graph-data-123-1', self.root)
        self.consumer = Path(self.temporary.name) / 'consumer'
        self.consumer.mkdir()
        with zipfile.ZipFile(self.root / 'output/adopt-snapshot.zip') as adoption:
            for name in adoption.namelist():
                target = self.consumer / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(adoption.read(name))

    def test_round_trip_all_sources_and_review_files(self):
        pages_snapshot.restore(self.archive, self.consumer)
        selection = json.loads((self.consumer / 'config/pages-snapshot.json').read_text())
        self.assertEqual(selection['release'], 'graph-data-123-1')
        self.assertEqual(set(selection['snapshots']), set(pages_snapshot.SOURCES))
        for dataset in pages_snapshot.SOURCES:
            relative = Path('data/snapshots/owid') / dataset
            pointer = self.consumer / 'output' / relative / 'current.yaml'
            self.assertEqual(yaml.safe_load(pointer.read_text()), yaml.safe_load((self.consumer / relative / 'current.yaml').read_text()))
            version = selection['snapshots'][dataset]['version']
            source = self.root / 'output' / relative / version / f'{dataset}.csv'
            self.assertEqual((self.consumer / 'output' / relative / version / source.name).read_bytes(), source.read_bytes())
        with zipfile.ZipFile(self.root / 'output/adopt-snapshot.zip') as adoption:
            self.assertFalse(any(name.endswith('.csv') for name in adoption.namelist()))

    def test_changed_archive_is_rejected_before_restore(self):
        self.archive.write_bytes(self.archive.read_bytes() + b'changed')
        with self.assertRaisesRegex(ValueError, 'archive checksum mismatch'):
            pages_snapshot.restore(self.archive, self.consumer)
        self.assertFalse((self.consumer / 'output/data/snapshots').exists())

    def test_selection_mismatch_and_existing_outputs_are_rejected(self):
        path = self.consumer / 'config/pages-snapshot.json'
        selection = json.loads(path.read_text())
        original = path.read_bytes()
        selection['snapshots']['headline']['version'] = 'wrong'
        path.write_text(json.dumps(selection))
        with self.assertRaisesRegex(ValueError, 'selected datasets'):
            pages_snapshot.restore(self.archive, self.consumer)
        path.write_bytes(original)
        pages_snapshot.restore(self.archive, self.consumer)
        with self.assertRaisesRegex(ValueError, 'already exists'):
            pages_snapshot.restore(self.archive, self.consumer)

    def test_missing_selection_never_falls_back_to_live_data(self):
        (self.consumer / 'config/pages-snapshot.json').unlink()
        with self.assertRaisesRegex(ValueError, 'No data snapshot selected'):
            pages_snapshot.restore(self.archive, self.consumer)

    def test_corrupt_source_is_rejected_before_candidate_archiving(self):
        source = next((self.root / 'output/data/snapshots/owid').rglob('headline.csv'))
        source.write_text('changed', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            pages_snapshot.prepare('graph-data-124-1', self.root)


class PreparedRunTests(unittest.TestCase):
    def test_accept_only_successful_main_preparation_runs(self):
        run = {'path': '.github/workflows/prepare-graphs.yml', 'head_branch': 'main',
               'event': 'push', 'status': 'completed', 'conclusion': 'success',
               'head_repository': {'full_name': 'owner/repo'}, 'head_sha': 'a' * 40, 'run_attempt': 2}
        self.assertEqual(pages_snapshot.validate_prepared_run(run, 'owner/repo'), ('a' * 40, 2))
        for key, value in (('path', '.github/workflows/refresh-graph-data.yml'),
                           ('head_branch', 'feature'), ('event', 'pull_request'),
                           ('status', 'in_progress'), ('conclusion', 'failure'),
                           ('head_repository', {'full_name': 'other/repo'}),
                           ('head_sha', 'not-a-sha'), ('run_attempt', 0)):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    pages_snapshot.validate_prepared_run(dict(run, **{key: value}), 'owner/repo')


if __name__ == '__main__':
    unittest.main()
