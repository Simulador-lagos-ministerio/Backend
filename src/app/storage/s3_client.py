"""Minimal S3-compatible client helpers (used by MinIO in dev/tests)."""
import os
import tempfile
from urllib.parse import urlparse
from pathlib import Path

import boto3

from app.settings import settings

def _get_s3():
    """Create a boto3 client using settings (endpoint, keys, region)."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )

def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an s3://bucket/key URI into (bucket, key)."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Unsupported URI scheme: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

def download_to_tempfile(uri: str) -> str:
    """Download an S3 object into a temporary file and return its path. User is responsible for cleanup by calling remove_tempfile()."""
    s3 = _get_s3()
    bucket, key = parse_s3_uri(uri)

    fd, path = tempfile.mkstemp(suffix=os.path.splitext(key)[1])
    os.close(fd)

    try:
        s3.download_file(bucket, key, path)
        return path
    except Exception:
        # Ensure we do not leak temp files on download failure.
        try:
            os.remove(path)
        except OSError:
            pass
        raise

def remove_tempfile(path: str) -> None:
    """Best-effort temp file cleanup."""
    try:
        p = Path(path).resolve()
        tmp = Path(tempfile.gettempdir()).resolve()

        # Ensure we only delete files inside the temp dir.
        if tmp not in p.parents and p.parent != tmp:
            return

        os.remove(p)
    except OSError:
        pass
