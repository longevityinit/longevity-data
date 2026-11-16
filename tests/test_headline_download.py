"""Recovery of the local pointer after an interrupted headline download."""
import datetime
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('headline_download', ROOT / 'src/pipeline/download/owid/headline.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class HeadlineDownloadTests(unittest.TestCase):
    def test_retry_repairs_missing_or_stale_pointer_without_redownloading(self):
        for stale in (False, True):
            with self.subTest(stale=stale), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                directory = root / 'output/data/snapshots/owid/headline'
                directory.mkdir(parents=True)
                pointer = directory / 'current.yaml'
                if stale:
                    pointer.write_text('version: 1900-01-01\n')
                with patch.object(module, 'ROOT', root), patch.object(
                    module, 'download_owid_chart_data', return_value=(b'fixture', {}, 'etag')
                ) as fetch:
                    with patch.object(module, 'write_current', side_effect=OSError('Interrupted pointer write')):
                        with self.assertRaises(OSError):
                            module.download('headline', 'fixture')
                    module.download('headline', 'fixture')
                    fetch.assert_called_once()
                current = yaml.safe_load(pointer.read_text())
                self.assertEqual(current['version'], datetime.date.today().isoformat())
                self.assertEqual(current['etag'], 'etag')
                self.assertTrue(current['csv_hash'])

    def test_explicit_versions_keep_same_day_refreshes_distinct_and_preserve_download_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(module, 'ROOT', root), patch.object(
                module, 'download_owid_chart_data', return_value=(b'fixture', {}, 'etag')
            ) as fetch:
                module.download('headline', 'fixture', '2026-11-13-123-1')
                module.download('headline', 'fixture', '2026-11-13-123-2')
                self.assertEqual(fetch.call_count, 2)
            directory = root / 'output/data/snapshots/owid/headline'
            self.assertTrue((directory / '2026-11-13-123-1/headline.csv').exists())
            record = yaml.safe_load((directory / '2026-11-13-123-2/headline.meta.yaml').read_text())
            self.assertEqual(record['download_date'], datetime.date.today().isoformat())
            self.assertEqual(yaml.safe_load((directory / 'current.yaml').read_text())['version'], '2026-11-13-123-2')

    def test_unsafe_version_fails_before_network_or_file_writes(self):
        with patch.object(module, 'download_owid_chart_data') as fetch:
            with self.assertRaises(ValueError):
                module.download('headline', 'fixture', '../escape')
            fetch.assert_not_called()
