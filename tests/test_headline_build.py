"""Standalone chart assets must remain complete and covered by publication checks."""
import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from utils.publication import validate_manifest, write_manifest

spec = importlib.util.spec_from_file_location('headline_chart_build', ROOT / 'src/pipeline/charts/headline.py')
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class HeadlineBuildTests(unittest.TestCase):
    def test_modules_and_styles_are_copied_and_verified_by_publication_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(ROOT / 'src/charts', root / 'src/charts')
            source = root / 'output/data/standardised/owid/headline'
            source.mkdir(parents=True)
            (source / 'headline.json').write_text('{"rows": []}')
            (source / 'observations.csv').write_text('code,year,value\n')
            write_manifest({p: p.name for p in source.iterdir()}, source / 'publish.json')

            def vendor_assets(directory):
                directory.mkdir(parents=True, exist_ok=True)
                for name in (builder.D3_FILENAME, 'd3-7.9.0.txt', 'd3-7.9.0-dependencies.txt'):
                    if name == 'd3-7.9.0-dependencies.txt':
                        shutil.copy2(ROOT / 'licenses' / name, directory / name)
                    else:
                        (directory / name).write_text('fixture')

            with patch.object(builder, 'ROOT', root), patch.object(builder, 'ensure_vendor_assets', vendor_assets):
                builder.main()
            directory = root / 'output/charts/headline-life-expectancy'
            manifest = directory / 'publish.json'
            verified = {path.name for path, _ in validate_manifest(manifest)}
            for asset in ('headline.js', 'headline-state.js', 'headline-controls.js', 'headline-renderer.js', 'headline.css'):
                self.assertIn(asset, verified)
                self.assertEqual((directory / asset).read_bytes(), (ROOT / 'src/charts' / asset).read_bytes())
            notice = root / 'output/charts/vendor/d3-7.9.0-dependencies.txt'
            self.assertIn(notice.name, verified)
            self.assertEqual(notice.read_bytes(), (ROOT / 'licenses' / notice.name).read_bytes())
            notice_text = notice.read_text(encoding='utf-8')
            self.assertIn('Copyright 2002 Cynthia Brewer', notice_text)
            self.assertIn('END OF TERMS AND CONDITIONS', notice_text)
            self.assertIn('APPENDIX: How to apply the Apache License', notice_text)
            (directory / 'headline.css').write_text('changed after build')
            with self.assertRaises(ValueError):
                validate_manifest(manifest)
