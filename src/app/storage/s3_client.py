import os
import tempfile
from urllib.parse import urlparse

import boto3
from app.settings import settings

def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )

def parse_s3_uri(uri: str) -> tuple[str, str]:
    # uri: s3://bucket/key
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Unsupported URI scheme: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

def download_to_tempfile(uri: str) -> str:
    s3 = _get_s3()
    bucket, key = parse_s3_uri(uri)

    fd, path = tempfile.mkstemp(suffix=os.path.splitext(key)[1])
    os.close(fd)

    s3.download_file(bucket, key, path)
    return path
