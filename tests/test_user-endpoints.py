from src.main import app
from src.database import SessionLocal

def test_signup(client):
    response = client.post(
        "/signup",
        json={"email": "api@example.com", "password": "password123"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "token_type" in data

def test_signup_duplicate(client):
    client.post("/signup", json={"email": "dup@example.com", "password": "password123"})
    response = client.post("/signup", json={"email": "dup@example.com", "password": "password123"})
    assert response.status_code == 400


def test_signin(client):
    client.post("/signup", json={"email": "login@example.com", "password": "password123"})
    response = client.post("/signin", json={"email": "login@example.com", "password": "password123"})
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_signin_wrong_password(client):
    client.post("/signup", json={"email": "login2@example.com", "password": "password123"})
    response = client.post("/signin", json={"email": "login2@example.com", "password": "wrong"})
    assert response.status_code == 401