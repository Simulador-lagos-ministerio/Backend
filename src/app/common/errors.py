from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AppError(Exception):
    """
    Typed application error used for consistent error responses.

    status_code:
      - Use 4xx for access/resource errors.
      - Use 5xx for internal errors.
    """
    code: str
    message: str
    status_code: int = 400
    meta: Optional[Dict[str, Any]] = None


# Convenience constructors (optional)
def not_found(code: str, message: str, meta: Optional[Dict[str, Any]] = None) -> AppError:
    return AppError(code=code, message=message, status_code=404, meta=meta)


def forbidden(code: str, message: str, meta: Optional[Dict[str, Any]] = None) -> AppError:
    return AppError(code=code, message=message, status_code=403, meta=meta)


def unauthorized(code: str, message: str, meta: Optional[Dict[str, Any]] = None) -> AppError:
    return AppError(code=code, message=message, status_code=401, meta=meta)

def bad_request(code: str, message: str, meta: Optional[Dict[str, Any]] = None) -> AppError:
    return AppError(code=code, message=message, status_code=400, meta=meta)

def internal(code: str, message: str, meta: Optional[Dict[str, Any]] = None) -> AppError:
    return AppError(code=code, message=message, status_code=500, meta=meta)
