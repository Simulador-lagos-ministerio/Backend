# tests/unit/users/test_users_model.py
"""
Unit tests for users model.

Focus:
- unique email constraint
- required fields
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from tests._resolve import resolve_db_symbols


def test_user_unique_email(db):
    """
    Insert two users with same email should raise IntegrityError.
    """
    # Resolve model dynamically (strictly enforce correct project structure).
    from app.users.models import User  # type: ignore

    u1 = User(email="unique@example.com", hashed_password="x")
    db.add(u1)
    db.commit()

    u2 = User(email="unique@example.com", hashed_password="y")
    db.add(u2)
    with pytest.raises(IntegrityError):
        db.commit()
