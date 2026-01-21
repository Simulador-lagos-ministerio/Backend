from app.users.schemas import Token, UserCreate, UserLogin


def test_user_create_schema():
    data = {
        "email": "test@example.com",
        "password": "secret123",
    }
    user = UserCreate(**data)

    assert user.email == data["email"]
    assert user.password == data["password"]


def test_user_login_schema():
    data = {
        "email": "test@example.com",
        "password": "secret123",
    }
    login = UserLogin(**data)

    assert login.email == data["email"]
    assert login.password == data["password"]


def test_token_schema():
    data = {
        "access_token": "fake-token",
        "token_type": "bearer",
    }
    token = Token(**data)

    assert token.access_token == data["access_token"]
    assert token.token_type == "bearer"
