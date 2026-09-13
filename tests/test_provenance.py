import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src/pipeline'))
from utils.publication import write_manifest, validate_manifest
from utils.provenance import prepare_metadata, require_committed_provenance, provenance_files
import publish


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.org')
        self.directory = self.root / 'output/data/snapshots/test/sample/2026-01-01'
        self.directory.mkdir(parents=True)
        self.meta = self.directory / 'sample.meta.yaml'
        self.meta.write_text('dataset: sample\n')
        self.licence = self.directory / 'sample.licence.txt'
        self.licence.write_text('fixture licence\n')
        self.manifest = self.directory / 'publish.json'
        write_manifest({self.meta: 'snapshots/test/sample/2026-01-01/sample.meta.yaml',
                        self.licence: 'snapshots/test/sample/2026-01-01/sample.licence.txt'}, self.manifest)
        self.target = self.root / 'data/snapshots/test/sample/2026-01-01/sample.meta.yaml'

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.root, check=True, capture_output=True)

    def test_requires_commit_and_unchanged_worktree_and_index(self):
        with self.assertRaises(ValueError):
            require_committed_provenance(self.manifest, self.root)
        prepare_metadata(self.manifest, self.root)
        self.git('add', 'data')
        with self.assertRaises(ValueError):
            require_committed_provenance(self.manifest, self.root)
        self.git('commit', '-qm', 'Record provenance')
        require_committed_provenance(self.manifest, self.root)
        self.target.write_text('changed\n')
        with self.assertRaises(ValueError):
            require_committed_provenance(self.manifest, self.root)
        self.git('add', 'data')
        self.target.write_bytes(self.meta.read_bytes())
        with self.assertRaises(ValueError):
            require_committed_provenance(self.manifest, self.root)

    def test_crlf_provenance_matches_git_after_recording(self):
        (self.root / '.gitattributes').write_bytes(b'* text=auto eol=lf\n')
        self.meta.write_bytes(b'dataset: sample\r\n')
        self.licence.write_bytes(b'fixture licence\r\nsecond line\r\n')
        pointer = self.directory.parent / 'current.yaml'
        pointer.write_bytes(b"version: '2026-01-01'\r\n")
        csv = self.directory / 'sample.csv'
        csv_bytes = b'year,value\r\n2020,42\r\n'
        csv.write_bytes(csv_bytes)
        prefix = 'snapshots/test/sample/2026-01-01/'
        write_manifest({self.meta: prefix + self.meta.name,
                        self.licence: prefix + self.licence.name,
                        csv: prefix + csv.name,
                        pointer: 'snapshots/test/sample/current.yaml'},
                       self.manifest, finalize=pointer)
        validate_manifest(self.manifest)
        records = provenance_files(self.manifest)
        for source in records.values():
            self.assertNotIn(b'\r\n', source.read_bytes())
        self.assertEqual(csv.read_bytes(), csv_bytes)
        # Only the frozen pointer is normalized; the original selection is untouched.
        self.assertIn(b'\r\n', pointer.read_bytes())
        prepare_metadata(self.manifest, self.root)
        self.git('add', '.gitattributes', 'data')
        self.git('commit', '-qm', 'Record normalized provenance')
        require_committed_provenance(self.manifest, self.root)
        self.assertEqual(self.git('status', '--porcelain', '--', 'data').stdout, b'')
        # Content changes still fail the byte-for-byte publication gate.
        self.target.write_bytes(b'dataset: different\n')
        with self.assertRaises(ValueError):
            require_committed_provenance(self.manifest, self.root)

    def test_conflicting_record_is_not_overwritten(self):
        prepare_metadata(self.manifest, self.root)
        self.target.write_text('existing record\n')
        with self.assertRaises(ValueError):
            prepare_metadata(self.manifest, self.root)
        self.assertEqual(self.target.read_text(), 'existing record\n')

    def test_chart_follows_pinned_snapshot_dependency(self):
        chart = self.root / 'output/chart.html'
        chart.write_text('fixture')
        manifest = self.root / 'output/publish.json'
        write_manifest({chart: 'charts/example/index.html'}, manifest, kind='chart', dependencies=[self.manifest])
        self.assertEqual(len(provenance_files(manifest)), 2)
        prepare_metadata(manifest, self.root)
        self.git('add', 'data')
        self.git('commit', '-qm', 'Record provenance')
        require_committed_provenance(manifest, self.root)
        self.manifest.write_text(self.manifest.read_text() + '\n')
        with self.assertRaisesRegex(ValueError, 'Source manifest changed'):
            require_committed_provenance(manifest, self.root)

    def test_publication_gate_runs_before_bucket_access(self):
        with patch.object(publish, 'get_storage_client') as client:
            with self.assertRaises(ValueError):
                publish.publish(self.manifest)
            client.assert_not_called()

    def test_chart_without_provenance_is_rejected(self):
        chart = self.root / 'output/chart.html'
        chart.write_text('fixture')
        manifest = self.root / 'output/publish.json'
        write_manifest({chart: 'charts/example/index.html'}, manifest, kind='chart')
        with self.assertRaises(ValueError):
            provenance_files(manifest)


if __name__ == '__main__':
    unittest.main()
