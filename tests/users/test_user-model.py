"""Unit tests for the User model."""
from typing import cast

import pytest
from passlib.hash import bcrypt

from app.users.models import User


class TestUserModel:
    """Test suite for User model."""

    def test_user_creation(self, db_session):
        """Create a user instance and persist it."""
        user = User(
            email="test@example.com",
            hashed_password=bcrypt.hash("testpassword123")
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        assert user.id is not None
        assert cast(str, user.email) == "test@example.com"
        assert user.hashed_password is not None

    def test_user_email_unique(self, db_session):
        """Email must be unique."""
        from sqlalchemy.exc import IntegrityError
        
        user1 = User(
            email="unique@example.com",
            hashed_password=bcrypt.hash("password1")
        )
        user2 = User(
            email="unique@example.com",
            hashed_password=bcrypt.hash("password2")
        )
        
        db_session.add(user1)
        db_session.commit()
        
        db_session.add(user2)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_verify_password_correct(self, db_session):
        """Password verification should succeed for correct password."""
        password = "correctpassword"
        user = User(
            email="verify@example.com",
            hashed_password=bcrypt.hash(password)
        )
        db_session.add(user)
        db_session.commit()

        assert user.verify_password(password) is True

    def test_verify_password_incorrect(self, db_session):
        """Password verification should fail for incorrect password."""
        user = User(
            email="verify2@example.com",
            hashed_password=bcrypt.hash("correctpassword")
        )
        db_session.add(user)
        db_session.commit()

        assert user.verify_password("wrongpassword") is False

    def test_user_table_name(self):
        """Table name is fixed."""
        assert User.__tablename__ == "users"

    def test_user_columns_exist(self):
        """Ensure required columns exist on the model."""
        assert hasattr(User, "id")
        assert hasattr(User, "email")
        assert hasattr(User, "hashed_password")
