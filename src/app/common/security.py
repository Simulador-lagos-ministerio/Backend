from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from jose import jwt
from passlib.hash import bcrypt

from app.settings import settings


def hash_password(password: str) -> str:
    """Hash a password using bcrypt (passlib)."""
    return bcrypt.hash(password)


def verify_password(password: str, hashed_password: str) -> bool:
    """Verify password against bcrypt hash."""
    return bcrypt.verify(password, hashed_password)


def create_access_token(*, subject: str, extra_claims: Optional[Dict[str, Any]] = None) -> str:
    """
    Create a short-lived JWT access token.
    subject should be the canonical user_id (UUID as string).
    """
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)

    payload: Dict[str, Any] = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
        "jti": secrets.token_urlsafe(16),
    }
    if extra_claims:
        payload.update(extra_claims)

    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def generate_refresh_token() -> str:
    """
    Generate a strong opaque refresh token.
    """
    return secrets.token_urlsafe(48)


def hash_refresh_token(token: str) -> str:
    """
    Store only a hash of the refresh token.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
