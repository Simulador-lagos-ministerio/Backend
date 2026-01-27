"""Unit tests for user service helpers."""
from typing import cast

import pytest

from app.users.services import authenticate_user, create_user


def test_create_user(db_session):
    email = "user@example.com"
    password = "password123"

    user = create_user(db_session, email, password)

    assert user.id is not None
    assert cast(str, user.email) == email
    assert cast(str, user.hashed_password) != password


def test_create_user_duplicate_email(db_session):
    email = "dup@example.com"
    password = "password123"

    create_user(db_session, email, password)

    with pytest.raises(ValueError):
        create_user(db_session, email, password)


def test_authenticate_user_success(db_session):
    email = "login@example.com"
    password = "password123"

    create_user(db_session, email, password)

    user = authenticate_user(db_session, email, password)

    assert user is not None
    assert user.email == email


def test_authenticate_user_wrong_password(db_session):
    email = "login@example.com"
    password = "password123"

    create_user(db_session, email, password)

    user = authenticate_user(db_session, email, "wrong")

    assert user is None
