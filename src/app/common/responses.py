from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


def ok(data: Any = None, message: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Standard success envelope.
    """
    payload: Dict[str, Any] = {
        "ok": True,
        "data": data,
        "error": None,
        "message": message,
    }
    if extra:
        payload.update(extra)
    return payload


def fail(
    code: str,
    message: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    data: Any = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Standard failure envelope (still a JSON payload).
    Use HTTP 200 for UX-friendly validation failures when appropriate.
    """
    payload: Dict[str, Any] = {
        "ok": False,
        "data": data,
        "error": {"code": code, "message": message, "meta": meta or {}},
        "message": message,  # Top-level message helps legacy frontends
    }
    if extra:
        payload.update(extra)
    return payload


def json_ok(data: Any = None, message: Optional[str] = None, extra: Optional[Dict[str, Any]] = None, status_code: int = 200) -> JSONResponse:
    payload = ok(data=data, message=message, extra=extra)
    return JSONResponse(content=jsonable_encoder(payload), status_code=status_code)


def json_fail(
    code: str,
    message: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    data: Any = None,
    extra: Optional[Dict[str, Any]] = None,
    status_code: int = 200,
) -> JSONResponse:
    payload = fail(code, message, meta=meta, data=data, extra=extra)
    return JSONResponse(content=jsonable_encoder(payload), status_code=status_code)
