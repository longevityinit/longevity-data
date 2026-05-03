import os
import boto3
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(raise_error_if_not_found=True))

def get_storage_client():
    """Returns an authenticated boto3 client for the remote object store."""
    if not all([os.environ.get("B2_BUCKET_NAME"), os.environ.get("B2_ENDPOINT_URL"),
                os.environ.get("B2_KEY_ID"), os.environ.get("B2_APPLICATION_KEY")]):
        raise ValueError("Missing storage environment variables. Check your .env file.")

    return boto3.client(
        service_name='s3',
        endpoint_url=os.environ.get("B2_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("B2_KEY_ID"),
        aws_secret_access_key=os.environ.get("B2_APPLICATION_KEY")
    )


def upload_map(base: Path, *paths: Path) -> dict[Path, str]:
    """Build a {local_path: storage_key} map by stripping `base` from each path.

    The bucket layout mirrors the project tree, so each pipeline stage just
    declares which files it produced and the helper derives the keys.
    """
    return {p: p.relative_to(base).as_posix() for p in paths}


def sync_to_storage(upload_map: dict[Path, str]):
    """Uploads a list of local file paths to the remote object store."""
    client = get_storage_client()
    bucket = os.environ.get("B2_BUCKET_NAME")

    for local_path, s3_key in upload_map.items():
        client.upload_file(str(local_path), bucket, s3_key)
        print(f"Uploaded {s3_key} to remote storage")
