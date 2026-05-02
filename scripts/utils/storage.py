import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Load storage configuration
def _find_project_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / ".git").exists():
            return p
    raise RuntimeError(f"Could not locate project root from {start}")

PROJECT_ROOT = _find_project_root(Path(__file__).resolve())
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

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

def sync_to_storage(upload_map: dict[Path, str]):
    """Uploads a list of local file paths to the remote object store."""
    client = get_storage_client()
    bucket = os.environ.get("B2_BUCKET_NAME")
    
    for local_path, s3_key in upload_map.items():
        client.upload_file(str(local_path), bucket, s3_key)
        print(f"Uploaded {s3_key} to remote storage")