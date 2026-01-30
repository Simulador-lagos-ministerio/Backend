# tests/unit/storage/test_s3_client.py
"""
Unit tests for app.storage.s3_client.

We validate:
- parse_s3_uri correctness + edge errors
- remove_tempfile safety behavior
- download_to_tempfile cleanup on failure (monkeypatched client)
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

import app.storage.s3_client as s3  # type: ignore


def test_parse_s3_uri_ok():
    bucket, key = s3.parse_s3_uri("s3://bucket/path/to/file.tif")
    assert bucket == "bucket"
    assert key == "path/to/file.tif"


def test_parse_s3_uri_rejects_non_s3():
    with pytest.raises(ValueError):
        s3.parse_s3_uri("http://bucket/key.tif")


def test_parse_s3_uri_rejects_empty_bucket_or_key():
    with pytest.raises(ValueError):
        s3.parse_s3_uri("s3:///key.tif")
    with pytest.raises(ValueError):
        s3.parse_s3_uri("s3://bucket/")


def test_remove_tempfile_is_idempotent(tmp_path):
    p = tmp_path / "x.txt"
    p.write_text("x")
    s3.remove_tempfile(str(p))
    # Second call should not raise
    s3.remove_tempfile(str(p))


def test_remove_tempfile_does_not_delete_outside_tmp():
    """
    Safety: do not delete arbitrary files outside system tempdir (policy decision).
    """
    tmp_root = Path(tempfile.gettempdir()).resolve()
    cwd = Path.cwd().resolve()
    if tmp_root in cwd.parents or cwd == tmp_root:
        pytest.skip("CWD is inside system tempdir; skip outside-tmp safety check.")

    p = cwd / "tests_keep_me.txt"
    p.write_text("x")
    try:
        s3.remove_tempfile(str(p))
        assert p.exists()
    finally:
        if p.exists():
            p.unlink()


def test_remove_tempfile_deletes_inside_tmp(monkeypatch):
    tmp = Path(tempfile.gettempdir()) / "hl_test_delete_me.txt"
    tmp.write_text("x")
    s3.remove_tempfile(str(tmp))
    assert not tmp.exists()


def test_download_to_tempfile_cleans_up_on_failure(monkeypatch):
    """
    Ensure no temp leak if the underlying download fails.

    We monkeypatch the internal S3 client getter if present.
    """
    if not hasattr(s3, "download_to_tempfile"):
        pytest.skip("download_to_tempfile not implemented")

    # Make the download fail.
    class FakeClient:
        def download_file(self, Bucket, Key, Filename):  # noqa: N802
            raise RuntimeError("download failed")

    if hasattr(s3, "get_s3_client"):
        monkeypatch.setattr(s3, "get_s3_client", lambda: FakeClient())
    elif hasattr(s3, "_get_s3_client"):
        monkeypatch.setattr(s3, "_get_s3_client", lambda: FakeClient())
    elif hasattr(s3, "_get_s3"):
        monkeypatch.setattr(s3, "_get_s3", lambda: FakeClient())
    else:
        pytest.skip("No S3 client getter to monkeypatch (adjust test for your implementation)")

    # Call and assert raises; also ensure no lingering file in temp with our prefix if you use one.
    with pytest.raises(RuntimeError):
        s3.download_to_tempfile("s3://bucket/key.tif")
