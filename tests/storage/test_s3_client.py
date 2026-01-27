"""Unit tests for S3 temp download helper."""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

import app.storage.s3_client as mod


class FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes]):
        self.objects = objects

    def download_file(self, bucket: str, key: str, filename: str):
        if (bucket, key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
        Path(filename).write_bytes(self.objects[(bucket, key)])


def test_download_to_tempfile_writes_file(monkeypatch):
    fake = FakeS3({("test", "ok.tif"): b"abc"})
    monkeypatch.setattr(mod.boto3, "client", lambda *_a, **_k: fake)

    path = mod.download_to_tempfile("s3://test/ok.tif")
    assert Path(path).exists()
    assert Path(path).read_bytes() == b"abc"


def test_download_to_tempfile_missing_object_raises(monkeypatch):
    fake = FakeS3({})
    monkeypatch.setattr(mod.boto3, "client", lambda *_a, **_k: fake)

    with pytest.raises(ClientError):
        mod.download_to_tempfile("s3://test/missing.tif")
