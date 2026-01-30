# tests/integration/users/test_users_endpoints.py
"""
Integration tests for /signup and /signin endpoints.
(Compatible with current frontend.)
"""

from __future__ import annotations


def test_signup_signin_flow(client):
    payload = {"email": "int1@example.com", "password": "StrongPass123!"}
    r1 = client.post("/signup", json=payload)
    assert r1.status_code in (200, 201)
    assert "access_token" in r1.json()

    r2 = client.post("/signin", json=payload)
    assert r2.status_code == 200
    assert "access_token" in r2.json()


def test_signin_missing_user_is_401(client):
    r = client.post("/signin", json={"email": "missing@example.com", "password": "StrongPass123!"})
    assert r.status_code == 401


def test_signup_invalid_email_is_422(client):
    r = client.post("/signup", json={"email": "not-an-email", "password": "StrongPass123!"})
    assert r.status_code == 422


def test_signup_weak_password_is_422(client):
    r = client.post("/signup", json={"email": "weakpass@example.com", "password": "123"})
    assert r.status_code == 422


