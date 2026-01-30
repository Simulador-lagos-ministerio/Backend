from __future__ import annotations

import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from uuid import UUID

from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.settings import settings
from app.common.errors import AppError, bad_request, unauthorized, not_found
from app.common.security import (
    hash_password,
    create_access_token,
    generate_refresh_token,
    hash_refresh_token,
)
from app.users.models import User, RefreshToken


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/signin")


def create_user(db: Session, email: str, password: str) -> User:
    """
    Create a new user. Raises a typed AppError on duplicates.
    """
    # Verify email is valid
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.fullmatch(regex, email):
        raise bad_request(code="INVALID_EMAIL", message="Invalid email address.", meta={"email": email})
    
    # Create user
    user = User(email=email, hashed_password=hash_password(password))
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise bad_request(code="EMAIL_ALREADY_REGISTERED", message="Email already registered.", meta={"email": email})
    db.refresh(user)
    return user


def authenticate_user(db: Session, email: str, password: str) -> Optional[User]:
    """
    Return the user if credentials are valid; otherwise None.
    """
    user = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if not user:
        return None
    if not user.verify_password(password):
        return None
    return user


def _refresh_expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)


def issue_token_pair(db: Session, user: User) -> Tuple[dict, str]:
    """
    Issue an access token (JWT) + refresh token (opaque, DB-backed).
    Returns (token_payload, refresh_token_plain).
    """
    access = create_access_token(
        subject=str(user.id),
        extra_claims={"email": user.email},
    )

    refresh_plain = generate_refresh_token()
    refresh_hash = hash_refresh_token(refresh_plain)
    jti = secrets.token_urlsafe(16)
    expires_at = _refresh_expiry()

    rt = RefreshToken(
        user_id=user.id,
        jti=jti,
        token_hash=refresh_hash,
        expires_at=expires_at,
        revoked=False,
        revoked_at=None,
        replaced_by_jti=None,
    )
    db.add(rt)
    db.commit()

    payload = {
        "access_token": access,
        "token_type": "bearer",
        "expires_in": settings.JWT_EXPIRE_MINUTES * 60,
        "refresh_token": refresh_plain,
        "refresh_expires_in": settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600,
    }
    return payload, refresh_plain


def rotate_refresh_token(db: Session, refresh_token_plain: str) -> dict:
    """
    Rotate a refresh token:
      - Verify current refresh token is valid and not revoked/expired.
      - Revoke old refresh token, set replaced_by_jti.
      - Issue new refresh token record and return new token pair payload.
    """
    now = datetime.now(timezone.utc)
    token_hash = hash_refresh_token(refresh_token_plain)

    old = db.execute(select(RefreshToken).where(RefreshToken.token_hash == token_hash)).scalar_one_or_none()
    if not old:
        raise unauthorized("INVALID_REFRESH_TOKEN", "Invalid refresh token.")
    if old.revoked:
        raise unauthorized("REFRESH_TOKEN_REVOKED", "Refresh token revoked.")
    if old.expires_at <= now:
        raise unauthorized("REFRESH_TOKEN_EXPIRED", "Refresh token expired.")

    user = db.execute(select(User).where(User.id == old.user_id)).scalar_one_or_none()
    if not user:
        # Defensive: should not happen if FK integrity holds
        raise not_found("USER_NOT_FOUND", "User not found.")

    # Revoke old
    old.revoked = True
    old.revoked_at = now

    # Issue new refresh token
    new_refresh_plain = generate_refresh_token()
    new_refresh_hash = hash_refresh_token(new_refresh_plain)
    new_jti = secrets.token_urlsafe(16)
    new_expires_at = _refresh_expiry()

    old.replaced_by_jti = new_jti

    new_rt = RefreshToken(
        user_id=user.id,
        jti=new_jti,
        token_hash=new_refresh_hash,
        expires_at=new_expires_at,
        revoked=False,
        revoked_at=None,
        replaced_by_jti=None,
    )
    db.add(new_rt)
    db.commit()

    access = create_access_token(subject=str(user.id), extra_claims={"email": user.email})
    return {
        "access_token": access,
        "token_type": "bearer",
        "expires_in": settings.JWT_EXPIRE_MINUTES * 60,
        "refresh_token": new_refresh_plain,
        "refresh_expires_in": settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600,
    }


def revoke_refresh_token(db: Session, refresh_token_plain: str) -> None:
    """
    Revoke a refresh token (idempotent).
    """
    token_hash = hash_refresh_token(refresh_token_plain)
    rt = db.execute(select(RefreshToken).where(RefreshToken.token_hash == token_hash)).scalar_one_or_none()
    if not rt:
        return
    if rt.revoked:
        return
    rt.revoked = True
    rt.revoked_at = datetime.now(timezone.utc)
    db.commit()


def get_current_user(db: Session, token: str) -> User:
    """
    Decode access token and load the user from DB.
    """
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        sub = payload.get("sub")
        if not sub:
            raise unauthorized("INVALID_TOKEN", "Invalid access token.")
        user_id = UUID(sub)
    except (JWTError, ValueError):
        raise unauthorized("INVALID_TOKEN", "Invalid access token.")

    user = db.execute(select(User).where(User.id == user_id)).scalar_one_or_none()
    if not user:
        raise unauthorized("USER_NOT_FOUND", "User not found.")
    return user
