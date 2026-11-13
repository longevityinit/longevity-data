"""Pages releases retain verified assets and reject changed or unsafe inputs."""
import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
import pages
from utils.publication import write_manifest


class PagesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        shutil.copytree(ROOT / 'src/site', self.root / 'src/site')
        shutil.copytree(ROOT / 'src/charts', self.root / 'src/charts')
        shutil.copytree(ROOT / 'src/pipeline/charts/templates', self.root / 'src/pipeline/charts/templates')
        shutil.copyfile(ROOT / 'LICENSE', self.root / 'LICENSE')
        (self.root / '.gitignore').write_text('/output/\n', encoding='utf-8')
        self.snapshot = self.root / 'output/data/snapshots/fixture/le/2020-01-01'
        self.snapshot.mkdir(parents=True)
        metadata = self.snapshot / 'le.meta.yaml'
        metadata.write_text('source: fixture\n', encoding='utf-8')
        pointer = self.snapshot.parent / 'current.yaml'
        pointer.write_text('version: 2020-01-01\n', encoding='utf-8')
        data = self.snapshot / 'le.csv'
        data.write_text('year,value\n2000,70\n', encoding='utf-8')
        source_manifest = self.snapshot / 'publish.json'
        write_manifest({metadata: 'snapshots/fixture/le/2020-01-01/le.meta.yaml',
                        pointer: 'snapshots/fixture/le/current.yaml',
                        data: 'snapshots/fixture/le/2020-01-01/le.csv'}, source_manifest)
        for relative, source in (
            ('data/snapshots/fixture/le/2020-01-01/le.meta.yaml', metadata),
            ('data/snapshots/fixture/le/current.yaml', pointer),
        ):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        self.git('init', '-q')
        self.git('add', '.')
        self.git('-c', 'user.name=Test', '-c', 'user.email=test@example.org', 'commit', '-qm', 'fixture')
        self.commit = self.git('rev-parse', 'HEAD').strip()
        self.chart = self.root / 'output/charts/headline-life-expectancy'
        self.chart.mkdir(parents=True)
        for name, source in pages.source_assets(self.root).items():
            shutil.copyfile(source, self.chart / name)
        (self.chart / 'headline.json').write_text(json.dumps({
            'code_revision': self.commit, 'code_dirty': False}), encoding='utf-8')
        vendor = self.chart.parent / 'vendor'
        vendor.mkdir()
        (vendor / 'd3.js').write_text('// fixture', encoding='utf-8')
        (vendor / 'licence.txt').write_text('Fixture licence', encoding='utf-8')
        self.manifest = self.chart / 'publish.json'
        self.assets = [*(self.chart / name for name in pages.source_assets(self.root)), self.chart / 'headline.json',
                       vendor / 'd3.js', vendor / 'licence.txt']
        self.dependency = source_manifest
        self.write_chart_manifest()

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.root, check=True,
                              capture_output=True, text=True).stdout

    def write_chart_manifest(self):
        write_manifest({p: p.relative_to(self.root / 'output').as_posix() for p in self.assets},
                       self.manifest, kind='chart', dependencies=[self.dependency])

    def test_round_trip_retains_relative_assets_licences_and_excludes_unlisted_files(self):
        (self.chart / 'secret.txt').write_text('not public', encoding='utf-8')
        archive = pages.build([self.manifest], self.root)
        destination = self.root / 'restored'
        pages.unpack(archive, destination, self.commit)
        self.assertEqual((destination / 'vendor/licence.txt').read_text(), 'Fixture licence')
        self.assertTrue((destination / 'headline-life-expectancy/index.html').is_file())
        self.assertIn('href="headline-life-expectancy/"', (destination / 'index.html').read_text())
        self.assertFalse((destination / 'headline-life-expectancy/secret.txt').exists())
        self.assertFalse((destination / 'data').exists())

    def test_stale_presentation_is_rebuilt_from_committed_sources(self):
        for name in ('headline-renderer.js', 'headline.css', 'index.html'):
            (self.chart / name).write_text('stale presentation', encoding='utf-8')
        # Simulate a legitimate but older chart build with intact manifest hashes
        # alongside indicator data claiming the current release revision.
        self.write_chart_manifest()
        archive = pages.build([self.manifest], self.root)
        destination = self.root / 'restored'
        pages.unpack(archive, destination, self.commit)
        for name, source in pages.source_assets(self.root).items():
            self.assertEqual((destination / self.chart.name / name).read_bytes(), source.read_bytes())

    def test_changed_asset_is_rejected_before_replacing_site(self):
        pages.build([self.manifest], self.root)
        previous = (self.root / 'output/pages/release.json').read_bytes()
        (self.chart / 'index.html').write_text('changed', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'changed artifact'):
            pages.build([self.manifest], self.root)
        self.assertEqual((self.root / 'output/pages/release.json').read_bytes(), previous)

    def test_uncommitted_source_provenance_is_rejected(self):
        (self.root / 'data/snapshots/fixture/le/current.yaml').write_text('changed', encoding='utf-8')
        with patch.object(pages, 'revision', return_value=self.commit):
            with self.assertRaisesRegex(ValueError, 'Provenance is missing'):
                pages.build([self.manifest], self.root)

    def test_dirty_checkout_and_stale_indicator_revision_are_rejected(self):
        (self.root / 'src/site/site.css').write_text('changed', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'checkout changes'):
            pages.build([self.manifest], self.root)
        self.git('checkout', '--', 'src/site/site.css')
        (self.chart / 'headline.json').write_text('{"code_revision":"old","code_dirty":false}', encoding='utf-8')
        self.write_chart_manifest()
        with self.assertRaisesRegex(ValueError, 'Rebuild headline'):
            pages.build([self.manifest], self.root)

    def test_refresh_candidate_cannot_be_deployed(self):
        archive = pages.build([self.manifest], self.root, candidate=True)
        destination = self.root / 'candidate-deploy'
        with self.assertRaisesRegex(ValueError, 'Candidate snapshots cannot be deployed'):
            pages.unpack(archive, destination, self.commit)
        self.assertFalse(destination.exists())

    def test_wrong_release_revision_is_rejected_without_extraction(self):
        archive = pages.build([self.manifest], self.root)
        destination = self.root / 'restored'
        with self.assertRaisesRegex(ValueError, 'source revision'):
            pages.unpack(archive, destination, 'wrong')
        self.assertFalse(destination.exists())

    def test_archive_traversal_links_duplicate_and_tampering_are_rejected(self):
        archive = pages.build([self.manifest], self.root)
        with tarfile.open(archive) as source:
            original = [(m.name, source.extractfile(m).read()) for m in source.getmembers()]
        for attack in ('../escaped', '/escaped', 'link', 'duplicate', 'tamper', 'extra'):
            with self.subTest(attack=attack):
                bad = self.root / 'bad.tar.gz'
                with tarfile.open(bad, 'w:gz') as bundle:
                    for name, content in original:
                        if attack == 'tamper' and name == 'index.html':
                            content = b'changed'
                        item = tarfile.TarInfo(name)
                        item.size = len(content)
                        bundle.addfile(item, io.BytesIO(content))
                    if attack != 'tamper':
                        item = tarfile.TarInfo('index.html' if attack == 'duplicate' else attack)
                        if attack == 'link':
                            item.type = tarfile.SYMTYPE
                            item.linkname = '/etc/passwd'
                        bundle.addfile(item, io.BytesIO())
                destination = self.root / 'restored'
                with self.assertRaises(ValueError):
                    pages.unpack(bad, destination, self.commit)
                self.assertFalse(destination.exists())


if __name__ == '__main__':
    unittest.main()
