import pytest
import sys
from pathlib import Path

# Add src to path FIRST
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

# Import using the same style as conftest
from models import User
from passlib.hash import bcrypt


class TestUserModel:
    """Test suite for User model"""

    def test_user_creation(self, db_session):
        """Test creating a user instance"""
        user = User(
            email="test@example.com",
            hashed_password=bcrypt.hash("testpassword123")
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        assert user.id is not None
        assert user.email == "test@example.com"
        assert user.hashed_password is not None

    def test_user_email_unique(self, db_session):
        """Test that email field is unique"""
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
        """Test password verification with correct password"""
        password = "correctpassword"
        user = User(
            email="verify@example.com",
            hashed_password=bcrypt.hash(password)
        )
        db_session.add(user)
        db_session.commit()

        assert user.verify_password(password) is True

    def test_verify_password_incorrect(self, db_session):
        """Test password verification with incorrect password"""
        user = User(
            email="verify2@example.com",
            hashed_password=bcrypt.hash("correctpassword")
        )
        db_session.add(user)
        db_session.commit()

        assert user.verify_password("wrongpassword") is False

    def test_user_table_name(self):
        """Test that table name is correctly set"""
        assert User.__tablename__ == "users"

    def test_user_columns_exist(self):
        """Test that all required columns exist"""
        assert hasattr(User, 'id')
        assert hasattr(User, 'email')
        assert hasattr(User, 'hashed_password')