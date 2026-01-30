"""
Unit tests for response helpers (ok/fail/json_ok/json_fail).
"""

from __future__ import annotations

import json

from app.common import responses


def _unwrap(payload):
    if hasattr(payload, "body"):
        return json.loads(payload.body)
    return payload


def test_ok_minimal_shape():
    payload = responses.ok(data={"a": 1})
    assert payload["ok"] is True
    assert payload["data"] == {"a": 1}
    assert payload["error"] is None


def test_fail_minimal_shape():
    payload = responses.fail(code="E", message="M")
    assert payload["ok"] is False
    assert payload["error"]["code"] == "E"
    assert payload["error"]["message"] == "M"


def test_json_ok_wraps_ok_and_status():
    res = responses.json_ok(data={"x": 2}, status_code=201)
    payload = _unwrap(res)
    assert payload["ok"] is True
    assert payload["data"] == {"x": 2}
    assert res.status_code == 201


def test_json_fail_wraps_fail_and_status():
    res = responses.json_fail(code="ERR", message="boom", status_code=400)
    payload = _unwrap(res)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "ERR"
    assert res.status_code == 400
