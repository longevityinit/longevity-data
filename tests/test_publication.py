import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/pipeline'))
from utils.publication import write_manifest, validate_manifest, snapshot_manifest

spec = importlib.util.spec_from_file_location('publisher', ROOT / 'src/pipeline/publish.py')
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.uploads = []
        self.fail_key = None

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return self.objects[Key]

    def upload_file(self, source, bucket, key, ExtraArgs):
        if key == self.fail_key:
            raise RuntimeError('Simulated failed upload')
        self.uploads.append(key)
        self.objects[key] = {'Metadata': ExtraArgs['Metadata'], 'ContentType': ExtraArgs['ContentType'],
                             'ContentLength': Path(source).stat().st_size}


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data.csv'
        self.data.write_text('value\n42\n')
        self.meta = self.root / 'meta.yaml'
        self.meta.write_text('unit: years\n')
        self.pointer = self.root / 'current.yaml'
        self.pointer.write_text('version: first\n')
        self.manifest = self.root / 'version/publish.json'
        # Deliberately put the pointer first to exercise finalization ordering.
        write_manifest({self.pointer: 'current.yaml', self.data: 'data.csv', self.meta: 'meta.yaml'},
                       self.manifest, finalize=self.pointer)
        gate = patch.object(publisher, 'require_committed_provenance')
        gate.start()
        self.addCleanup(gate.stop)
        self.client = FakeStorage()
        patcher = patch.object(publisher, 'get_storage_client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = patch.dict(os.environ, {'B2_BUCKET_NAME': 'test', 'B2_ENDPOINT_URL': 'https://example.org'})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_failure_and_retry_publish_pointer_last(self):
        self.client.fail_key = 'meta.yaml'
        with self.assertRaisesRegex(RuntimeError, 'Simulated'):
            publisher.publish(self.manifest)
        self.assertEqual(self.client.uploads, ['data.csv'])
        receipt = json.loads(self.manifest.with_name('publish.receipt.json').read_text())
        self.assertEqual(receipt['completed'], ['data.csv'])
        self.assertFalse(receipt['complete'])
        self.client.fail_key = None
        publisher.publish(self.manifest)
        self.assertEqual(self.client.uploads, ['data.csv', 'meta.yaml', 'current.yaml'])
        publisher.publish(self.manifest)
        self.assertEqual(len(self.client.uploads), 3)
        # A saved receipt cannot hide a missing or replaced remote object.
        del self.client.objects['data.csv']
        publisher.publish(self.manifest)
        self.assertEqual(self.client.uploads[-1], 'data.csv')

    def test_changed_or_missing_input_prevents_any_remote_access(self):
        for missing in (False, True):
            if missing:
                self.meta.unlink()
            else:
                self.meta.write_text('unit: changed\n')
            with patch.object(publisher, 'get_storage_client') as client:
                with self.assertRaises(ValueError):
                    publisher.publish(self.manifest)
                client.assert_not_called()

    def test_pointer_is_frozen_and_dry_run_needs_no_credentials(self):
        self.pointer.write_text('version: later\n')
        entries = validate_manifest(self.manifest)
        self.assertEqual(entries[-1][0].read_text(), 'version: first\n')
        with patch.object(publisher, 'get_storage_client') as client:
            publisher.publish(self.manifest, dry_run=True)
            client.assert_not_called()
        self.assertFalse(self.manifest.with_name('publish.receipt.json').exists())

    def test_existing_snapshot_manifest_can_be_recreated(self):
        import xxhash
        import yaml
        directory = self.root / 'data/snapshots/fixture/deaths/2026-01-01'
        directory.mkdir(parents=True)
        csv = directory / 'deaths.csv'
        csv.write_text('year,val\n2020,42\n')
        (directory / 'deaths.meta.yaml').write_text(yaml.safe_dump({
            'source': 'fixture', 'download_method': 'manual',
            'checksums': {'csv_xxh3_64': xxhash.xxh3_64_hexdigest(csv.read_bytes())}}))
        current = directory.parent / 'current.yaml'
        current.write_text('version: newer\n')
        manifest = snapshot_manifest(directory, self.root / 'data')
        entries = validate_manifest(manifest)
        self.assertEqual(current.read_text(), 'version: newer\n')
        self.assertEqual(yaml.safe_load(entries[-1][0].read_text())['version'], '2026-01-01')
        self.assertEqual(entries[-1][1]['key'], 'snapshots/fixture/deaths/current.yaml')

    def test_failed_verification_does_not_publish_pointer(self):
        with patch.object(self.client, 'head_object', side_effect=ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )):
            with self.assertRaisesRegex(RuntimeError, 'verification failed'):
                publisher.publish(self.manifest)
        self.assertEqual(self.client.uploads, ['data.csv'])

    def test_permission_error_is_not_treated_as_missing_object(self):
        with patch.object(self.client, 'head_object', side_effect=ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'HeadObject'
        )):
            with self.assertRaises(ClientError):
                publisher.publish(self.manifest)
        self.assertEqual(self.client.uploads, [])


if __name__ == '__main__':
    unittest.main()
