"""Explicit, resumable publication of an existing build."""
import argparse
import os
from pathlib import Path

from botocore.exceptions import ClientError

from utils.publication import checksum, validate_manifest, write_json
from utils.storage import get_storage_client
from utils.provenance import require_committed_provenance


def remote_matches(client, bucket: str, entry: dict) -> bool:
    try:
        remote = client.head_object(Bucket=bucket, Key=entry['key'])
    except ClientError as error:
        if error.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise
    return (remote.get('Metadata', {}).get('sha256') == entry['sha256']
            and remote.get('ContentLength') == entry['size']
            and remote.get('ContentType') == entry['content_type'])


def upload_artifact(client, bucket: str, source: Path, entry: dict):
    """Skip matching remote objects; otherwise upload and verify before continuing."""
    if remote_matches(client, bucket, entry):
        print(f"Already published: {entry['key']}")
        return

    client.upload_file(
        str(source), bucket, entry['key'],
        ExtraArgs={'Metadata': {'sha256': entry['sha256']}, 'ContentType': entry['content_type']},
    )
    if not remote_matches(client, bucket, entry):
        raise RuntimeError(f"Remote verification failed: {entry['key']}")
    print(f"Uploaded {entry['key']}")


def new_receipt(manifest_path: Path, bucket: str) -> dict:
    return {
        'manifest_sha256': checksum(manifest_path),
        'bucket': bucket,
        'endpoint': os.environ['B2_ENDPOINT_URL'],
        'completed': [],
        'complete': False,
    }


def publish(manifest_path: Path, *, dry_run: bool = False):
    # Validate every artifact before creating a client or making any remote writes.
    manifest_path = manifest_path.resolve()
    entries = validate_manifest(manifest_path)
    if dry_run:
        for source, entry in entries:
            print(f"{source} -> {entry['key']}" + (' (publish last)' if entry['finalize'] else ''))
        return

    require_committed_provenance(manifest_path, Path(__file__).resolve().parents[2])
    client = get_storage_client()
    bucket = os.environ['B2_BUCKET_NAME']
    receipt = new_receipt(manifest_path, bucket)
    receipt_path = manifest_path.with_name('publish.receipt.json')
    write_json(receipt_path, receipt)
    for source, entry in entries:
        upload_artifact(client, bucket, source, entry)
        receipt['completed'].append(entry['key'])
        write_json(receipt_path, receipt)
    receipt['complete'] = True
    write_json(receipt_path, receipt)
    print(f'Publication complete. Receipt: {receipt_path}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true', help='Validate and list destinations without bucket access.')
    args = parser.parse_args()
    publish(args.manifest, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
