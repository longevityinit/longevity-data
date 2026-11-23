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
import pages_review
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

    def test_seven_dataset_snapshot_adopts_and_restores_without_changing_legacy_set(self):
        snapshot_root = self.root / 'output/data/snapshots/owid'
        dependencies = []
        for dataset in set(pages_snapshot.ALL_SOURCES) - set(pages_snapshot.SOURCES):
            directory = snapshot_root / dataset
            shutil.copytree(snapshot_root / 'headline', directory)
            version = directory / '2026-11-13-123-1'
            for path in list(version.glob('headline.*')):
                path.rename(version / path.name.replace('headline.', dataset + '.', 1))
            dependencies.append(snapshot_manifest(version, self.root / 'output/data'))
        chart = self.root / 'output/charts/headline-life-expectancy-age-65'
        chart.mkdir()
        (chart / 'index.html').write_text('age65 fixture', encoding='utf-8')
        write_manifest({chart / 'index.html': 'charts/headline-life-expectancy-age-65/index.html'},
                       chart / 'publish.json', kind='chart', dependencies=dependencies)
        archive = pages_snapshot.prepare('graph-data-124-1', self.root, include_age65=True)
        destination = Path(self.temporary.name) / 'seven-datasets'
        destination.mkdir()
        pages_review.apply_adoption(self.root / 'output/adopt-snapshot.zip', destination)
        selection = json.loads((destination / 'config/pages-snapshot.json').read_text())
        self.assertEqual(set(selection['snapshots']), set(pages_snapshot.ALL_SOURCES))
        self.assertEqual(len(list(destination.rglob('*.meta.yaml'))), 7)
        pages_snapshot.restore(archive, destination)
        self.assertEqual(len(list((destination / 'output/data/snapshots').rglob('current.yaml'))), 7)
        self.assertEqual(len(pages_snapshot.SOURCES), 5)

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

    def test_adoption_application_accepts_only_selected_provenance(self):
        target = Path(self.temporary.name) / 'adoption'
        target.mkdir()
        pages_review.apply_adoption(self.root / 'output/adopt-snapshot.zip', target)
        self.assertEqual((target / 'config/pages-snapshot.json').read_bytes(),
                         (self.consumer / 'config/pages-snapshot.json').read_bytes())
        self.assertEqual(len(list(target.rglob('*.meta.yaml'))), 5)
        self.assertFalse(list(target.rglob('*.csv')))

    def test_adoption_unrelated_paths_and_conflicts_fail_before_changes(self):
        original = self.root / 'output/adopt-snapshot.zip'
        for extra in ('../escape', '.github/workflows/injected.yml'):
            with self.subTest(extra=extra):
                malicious = self.root / 'output/malicious.zip'
                shutil.copyfile(original, malicious)
                with zipfile.ZipFile(malicious, 'a') as bundle:
                    bundle.writestr(extra, 'unrelated')
                destination = Path(self.temporary.name) / 'rejected'
                destination.mkdir(exist_ok=True)
                with self.assertRaises(ValueError):
                    pages_review.apply_adoption(malicious, destination)
                self.assertFalse(list(destination.iterdir()))
        immutable = next((self.consumer / 'data').rglob('*.meta.yaml'))
        immutable.write_text('conflicting metadata')
        config = self.consumer / 'config/pages-snapshot.json'
        config.write_text('unchanged sentinel')
        with self.assertRaisesRegex(ValueError, 'immutable provenance'):
            pages_review.apply_adoption(original, self.consumer)
        self.assertEqual(config.read_text(), 'unchanged sentinel')


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


class AutomaticRunTests(unittest.TestCase):
    def run_record(self, run_id=2, sha='a' * 40, **overrides):
        return dict({'id': run_id, 'path': '.github/workflows/prepare-graphs.yml',
                     'head_branch': 'main', 'head_sha': sha, 'event': 'push',
                     'status': 'completed', 'conclusion': 'success',
                     'head_repository': {'full_name': 'owner/repo'}, 'run_attempt': 1}, **overrides)

    def test_current_commit_is_selected_even_when_other_commits_are_newer(self):
        records = [self.run_record(1), self.run_record(9, 'b' * 40), self.run_record(3, run_attempt=2)]
        selected = pages_snapshot.select_prepared_run(records, 'owner/repo', 'a' * 40)
        self.assertEqual((selected['id'], selected['run_attempt']), (3, 2))

    def test_old_success_is_not_a_fallback_for_missing_or_failed_current_main(self):
        old = self.run_record(1, 'b' * 40)
        with self.assertRaisesRegex(ValueError, 'No prepared build'):
            pages_snapshot.select_prepared_run([old], 'owner/repo', 'a' * 40)
        for state in ({'status': 'in_progress', 'conclusion': None}, {'conclusion': 'failure'},
                      {'conclusion': 'cancelled'}, {'head_repository': {'full_name': 'foreign/repo'}}):
            with self.subTest(state=state):
                with self.assertRaisesRegex(ValueError, 'not successful'):
                    pages_snapshot.select_prepared_run([old, self.run_record(2), self.run_record(3, **state)], 'owner/repo', 'a' * 40)

    def test_foreign_workflows_and_pull_requests_are_not_candidates(self):
        records = [self.run_record(3, path='.github/workflows/refresh-graph-data.yml'),
                   self.run_record(4, event='pull_request')]
        with self.assertRaisesRegex(ValueError, 'No prepared build'):
            pages_snapshot.select_prepared_run(records, 'owner/repo', 'a' * 40)


class DataReviewTests(unittest.TestCase):
    def graph(self, rows):
        return {'start_year': 1950, 'end_year': 2023, 'rows': [
            {'sex': 'female', 'year': year, 'series': 'USA', 'value': value} for year, value in rows]}

    def test_report_distinguishes_changes_added_removed_and_missing(self):
        before = self.graph([(1950, 70), (1951, 71), (1952, None), (1953, 74)])
        after = self.graph([(1950, 72), (1951, None), (1952, 73), (1954, 75)])
        report = pages_review.summarize(before, after)
        self.assertIn('Changed values: **3**', report)
        self.assertIn('Added rows: **1**; removed rows: **1**', report)
        self.assertIn('Newly missing values: **1**; newly available values: **1**', report)
        self.assertIn('**2 years**', report)

    def test_report_has_an_explicit_first_snapshot_case(self):
        self.assertIn('No previous selected snapshot', pages_review.summarize(None, self.graph([(1950, 70)])))
        with self.assertRaises(ValueError):
            pages_review.summarize(None, self.graph([(1950, 70), (1950, 71)]))


if __name__ == '__main__':
    unittest.main()
