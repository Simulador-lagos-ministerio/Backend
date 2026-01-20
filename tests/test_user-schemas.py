import pytest

from pydantic import ValidationError
from src.schemas import UserBase


class TestUserBaseSchema:
    """Test suite for UserBase schema"""

    def test_valid_user_base(self):
        """Test creating UserBase with valid data"""
        user = UserBase(email="test@example.com")
        assert user.email == "test@example.com"

    def test_user_base_with_various_emails(self):
        """Test UserBase accepts various valid email formats"""
        valid_emails = [
            "simple@example.com",
            "very.common@example.com",
            "disposable.style.email.with+symbol@example.com",
            "user@subdomain.example.com",
            "user123@example.co.uk"
        ]
        
        for email in valid_emails:
            user = UserBase(email=email)
            assert user.email == email

    def test_user_base_missing_email(self):
        """Test that UserBase requires email field"""
        with pytest.raises(ValidationError) as exc_info:
            UserBase()
        
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]['loc'] == ('email',)
        assert errors[0]['type'] == 'missing'

    def test_user_base_email_type_validation(self):
        """Test that email must be a string"""
        with pytest.raises(ValidationError):
            UserBase(email=12345)

    def test_user_base_to_dict(self):
        """Test converting UserBase to dictionary"""
        user = UserBase(email="dict@example.com")
        user_dict = user.model_dump()
        
        assert isinstance(user_dict, dict)
        assert user_dict['email'] == "dict@example.com"

    def test_user_base_from_dict(self):
        """Test creating UserBase from dictionary"""
        data = {"email": "fromdict@example.com"}
        user = UserBase(**data)
        
        assert user.email == "fromdict@example.com"

    def test_user_base_json_serialization(self):
        """Test JSON serialization of UserBase"""
        user = UserBase(email="json@example.com")
        json_str = user.model_dump_json()
        
        assert isinstance(json_str, str)
        assert "json@example.com" in json_str