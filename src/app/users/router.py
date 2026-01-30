from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.postgis_database import get_db
from app.common.responses import json_ok, json_fail
from app.common.errors import AppError
from app.users.schemas import UserCreate, UserLogin, RefreshRequest, LogoutRequest
from app.users.services import (
    create_user,
    authenticate_user,
    issue_token_pair,
    rotate_refresh_token,
    revoke_refresh_token,
    oauth2_scheme,
    get_current_user,
)

router = APIRouter()


@router.post("/signup")
def signup(payload: UserCreate, db: Session = Depends(get_db)):
    """
    Backward-compatible signup:
      - Keeps /signup route unchanged.
      - Returns access_token and token_type at top-level for legacy frontend.
      - Also returns standardized envelope with tokens in data.
    """
    user = create_user(db, payload.email, payload.password)
    token_payload, _ = issue_token_pair(db, user)

    extra = {
        "access_token": token_payload["access_token"],
        "token_type": token_payload["token_type"],
    }
    return json_ok(data=token_payload, extra=extra)


@router.post("/signin")
def signin(payload: UserLogin, db: Session = Depends(get_db)):
    """
    Backward-compatible signin (same contract as signup).
    """
    user = authenticate_user(db, payload.email, payload.password)
    if not user:
        # Keep 401 for auth failures (not UX-drawing validation)
        raise AppError(code="INVALID_CREDENTIALS", message="Invalid credentials.", status_code=401)

    token_payload, _ = issue_token_pair(db, user)
    extra = {
        "access_token": token_payload["access_token"],
        "token_type": token_payload["token_type"],
    }
    return json_ok(data=token_payload, extra=extra)


@router.post("/refresh")
def refresh(payload: RefreshRequest, db: Session = Depends(get_db)):
    """
    Rotate refresh token and return new access+refresh pair.
    Refresh token is sent in JSON body (Option B).
    """
    token_payload = rotate_refresh_token(db, payload.refresh_token)
    extra = {
        "access_token": token_payload["access_token"],
        "token_type": token_payload["token_type"],
    }
    return json_ok(data=token_payload, extra=extra)


@router.post("/logout")
def logout(payload: LogoutRequest, db: Session = Depends(get_db)):
    """
    Revoke a refresh token (idempotent).
    """
    revoke_refresh_token(db, payload.refresh_token)
    return json_ok(data={"revoked": True})


@router.get("/me")
def me(
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme),
):
    """
    Return current user identity.
    """
    user = get_current_user(db, token)
    return json_ok(data={"id": str(user.id), "email": user.email})
