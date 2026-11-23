"""Standalone chart assets must remain complete and covered by publication checks."""
import importlib.util
import re
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
            shutil.copytree(ROOT / 'src/pipeline/charts/templates', root / 'src/pipeline/charts/templates')
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
                age_source = root / 'output/data/standardised/owid/headline_age65'
                shutil.copytree(source, age_source)
                write_manifest({p: p.name for p in age_source.iterdir() if p.name != 'publish.json'}, age_source / 'publish.json')
                builder.main(65)
                age_chart = root / 'output/charts/headline-life-expectancy-age-65'
                age_assets = {p.name for p, _ in validate_manifest(age_chart / 'publish.json')}
                self.assertIn('headline-config.js', age_assets)
                self.assertIn('d3-7.9.0-dependencies.txt', age_assets)
                html = (age_chart / 'index.html').read_text(encoding='utf-8')
                self.assertIn('Remaining life expectancy at age 65', html)
                self.assertIn('65 or older', html)
                self.assertLess(html.index('id="methodology"'), html.index('</dialog>'))
                self.assertLess(html.index('Sources and credits'), html.index('</dialog>'))
                self.assertNotIn('age 65 is not', html)
            directory = root / 'output/charts/headline-life-expectancy'
            manifest = directory / 'publish.json'
            verified = {path.name for path, _ in validate_manifest(manifest)}
            for asset in ('headline-config.js', 'headline.js', 'headline-state.js', 'headline-controls.js', 'headline-renderer.js', 'headline.css'):
                self.assertIn(asset, verified)
                built = (directory / asset).read_text(encoding='utf-8')
                self.assertEqual(re.sub(r'\?v=[a-f0-9]{12}', '', built),
                                 (ROOT / 'src/charts' / asset).read_text(encoding='utf-8'))
            html = (directory / 'index.html').read_text(encoding='utf-8')
            version = re.search(r'headline.js\?v=([a-f0-9]{12})', html).group(1)
            self.assertIn(f'headline.css?v={version}', html)
            self.assertIn(f'headline-controls.js?v={version}',
                          (directory / 'headline.js').read_text(encoding='utf-8'))
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
