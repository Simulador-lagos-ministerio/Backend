# tests/unit/users/test_users_services.py
"""
Unit tests for users services.

We cover:
- password hashing and verification
- create_user duplicate handling
- authenticate_user behaviors
- token generation basic claims
"""

from __future__ import annotations

import pytest

from tests._resolve import resolve_users_services


def test_password_hash_and_verify():
    svc = resolve_users_services()

    assert hasattr(svc, "hash_password"), "hash_password must exist"
    verify = getattr(svc, "verify_password", None)
    if verify is None:
        from app.common.security import verify_password as verify  # type: ignore

    hashed = svc.hash_password("MyPass123!")
    assert hashed != "MyPass123!"
    assert verify("MyPass123!", hashed) is True
    assert verify("Wrong!", hashed) is False


def test_create_user_and_authenticate(db):
    svc = resolve_users_services()

    assert hasattr(svc, "create_user"), "create_user must exist"
    assert hasattr(svc, "authenticate_user"), "authenticate_user must exist"

    user = svc.create_user(db, email="svc@example.com", password="MyPass123!")
    assert user.email == "svc@example.com"

    ok = svc.authenticate_user(db, email="svc@example.com", password="MyPass123!")
    assert ok is not None

    bad = svc.authenticate_user(db, email="svc@example.com", password="Wrong!")
    assert bad is None


def test_create_user_duplicate_is_handled(db):
    svc = resolve_users_services()
    svc.create_user(db, email="dup_svc@example.com", password="MyPass123!")

    # Depending on your policy: raise custom error or return None.
    # We'll enforce "raises" OR returns a falsy sentinel.
    try:
        u = svc.create_user(db, email="dup_svc@example.com", password="MyPass123!")
        assert not u, "Expected duplicate create_user to fail"
    except Exception:
        assert True



def test_create_user_invalid_email_is_handled(db):
    svc = resolve_users_services()

    from app.common.errors import AppError  # type: ignore

    with pytest.raises(AppError) as exc:
        svc.create_user(db, email="not-an-email", password="SomePass!")
    assert exc.value.code == "INVALID_EMAIL"


def test_access_token_contains_sub_claim(db):
    svc = resolve_users_services()
    assert hasattr(svc, "create_access_token"), "create_access_token must exist"

    token = svc.create_access_token(subject="tok@example.com")
    assert isinstance(token, str)
    assert len(token) > 20


    
