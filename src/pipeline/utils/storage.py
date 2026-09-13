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
