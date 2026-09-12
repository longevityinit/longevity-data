"""Exercise local stages with fixture downloads and no storage access."""
import re
import runpy
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from utils import storage


class LocalPipelineTests(unittest.TestCase):
    def test_local_never_creates_storage_client(self):
        with patch.object(storage, 'get_storage_client', side_effect=AssertionError('Cloud access')):
            storage.sync_to_storage({}, local=True)

    def test_default_still_uploads(self):
        client = Mock()
        with patch.object(storage, 'get_storage_client', return_value=client), patch.dict(
            'os.environ', {'B2_BUCKET_NAME': 'test-bucket'}
        ):
            storage.sync_to_storage({Path('sample.csv'): 'snapshots/sample.csv'})
        client.upload_file.assert_called_once_with('sample.csv', 'test-bucket', 'snapshots/sample.csv')

    def test_all_stages_without_dotenv_or_credentials(self):
        csv_bytes = b'Entity,Code,Year,Life expectancy\nWorld,OWID_WRL,2020,72.1234\n'
        metadata = {
            'chart': {'title': 'Fixture life expectancy', 'citation': 'Fixture source', 'selection': ['World']},
            'columns': {'Life expectancy': {'shortName': 'life_expectancy_0'}},
        }

        def response(url, **kwargs):
            result = Mock(status_code=200, headers={'ETag': 'fixture'})
            result.content = csv_bytes if url.endswith('.csv') else b'// fixture library'
            result.json.return_value = metadata
            return result

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(ROOT / 'src/pipeline', root / 'src/pipeline', ignore=shutil.ignore_patterns('__pycache__'))
            shutil.copytree(ROOT / 'src/charts', root / 'src/charts')
            self.assertFalse((root / '.env').exists())

            def run(relative, *args):
                path = root / relative
                with patch.object(sys, 'argv', [str(path), '--local', *args]):
                    runpy.run_path(str(path), run_name='__main__')

            with patch.object(storage, 'get_storage_client', side_effect=AssertionError('Cloud access')), patch(
                'requests.get', side_effect=response
            ), patch.object(storage, 'load_dotenv', side_effect=AssertionError('Read dotenv')):
                run('src/pipeline/download/owid/life_expectancy.py')
                run('src/pipeline/standardise/owid/life_expectancy.py')
                run('src/pipeline/charts/life_expectancy.py')
                manual = root / 'manual.csv'
                manual.write_text('location_name,year,val,upper,lower\nWorld,2020,10,12,8\n')
                run('src/pipeline/download/ingest_manual.py', '--source', 'fixture', '--dataset', 'deaths',
                    '--zip', str(manual), '--url', 'https://example.org/export')

            chart = root / 'output/charts/life-expectancy'
            self.assertIn('Fixture life expectancy', (chart / 'index.html').read_text())
            self.assertIn('72.12', (chart / 'life-expectancy_tli.csv').read_text())
            self.assertTrue((root / 'output/data/snapshots/fixture/deaths/current.yaml').exists())
            self.assertFalse((root / 'data').exists())
            self.assertFalse((root / 'charts/vendor').exists())
            for source in re.findall(r'<script src="([^"]+)"', (chart / 'index.html').read_text()):
                self.assertTrue((chart / source).is_file(), source)
            # Verify normal builds still consume the original data path and upload keys.
            shutil.copytree(root / 'output/data', root / 'data')
            shutil.copytree(root / 'output/charts/vendor', root / 'charts/vendor')
            chart = root / 'charts/life-expectancy'
            # Every browser script URL must exist locally and have a matching upload key.
            with patch.object(storage, 'sync_to_storage') as upload, patch(
                'requests.get', side_effect=AssertionError('Cached build fetched a library')
            ), patch.object(sys, 'argv', [str(root / 'src/pipeline/charts/life_expectancy.py')]):
                runpy.run_path(str(root / 'src/pipeline/charts/life_expectancy.py'), run_name='__main__')
            upload_map = upload.call_args.args[0]
            self.assertFalse(upload.call_args.kwargs['local'])
            sources = re.findall(r'<script src="([^"]+)"', (chart / 'index.html').read_text())
            self.assertEqual(len(sources), 3)
            for source in sources:
                asset = (chart / source).resolve()
                self.assertTrue(asset.is_file(), source)
                self.assertEqual(upload_map[asset], asset.relative_to(root).as_posix())
            self.assertTrue(sources[0].startswith('../vendor/'))
            self.assertTrue(sources[1].startswith('../vendor/'))
            self.assertEqual(sources[2], '../lib/longevityplot.js')
            self.assertEqual(list((root / 'charts/lib').iterdir()), [root / 'charts/lib/longevityplot.js'])



if __name__ == '__main__':
    unittest.main()
