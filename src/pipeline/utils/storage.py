import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


def get_storage_client():
    """Returns an authenticated boto3 client for the remote object store."""
    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    if not all([os.environ.get("B2_BUCKET_NAME"), os.environ.get("B2_ENDPOINT_URL"),
                os.environ.get("B2_KEY_ID"), os.environ.get("B2_APPLICATION_KEY")]):
        raise ValueError("Missing storage environment variables. Check your .env file.")

    return boto3.client(
        service_name='s3',
        endpoint_url=os.environ.get("B2_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("B2_KEY_ID"),
        aws_secret_access_key=os.environ.get("B2_APPLICATION_KEY")
    )


def sync_to_storage(upload_map: dict[Path, str], *, local: bool = False):
    """Upload files by object key, or skip storage access entirely in local mode."""
    if local:
        print("Local mode: artifacts saved; skipping storage upload.")
        return
    client = get_storage_client()
    bucket = os.environ.get("B2_BUCKET_NAME")

    for local_path, s3_key in upload_map.items():
        client.upload_file(str(local_path), bucket, s3_key)
        print(f"Uploaded {s3_key} to remote storage")
