from datetime import datetime, timedelta

from jose import jwt
from passlib.hash import bcrypt
from sqlalchemy.exc import IntegrityError

from app.users import models as _models

# Auth helpers and token utilities.
SECRET_KEY = "dev-secret"
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60 * 24 * 60  # Expires in 2 months

revoked_tokens = set()


def get_user_by_email(db, email: str):
    return db.query(_models.User).filter(_models.User.email == email).first()


def create_user(db, email: str, password: str):
    if get_user_by_email(db, email):
        raise ValueError("Email already registered")

    user = _models.User(
        email=email,
        hashed_password=bcrypt.hash(password),
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise ValueError("Email already registered")
    db.refresh(user)
    return user


def authenticate_user(db, email: str, password: str):
    user = get_user_by_email(db, email)
    if not user:
        return None
    if not user.verify_password(password):
        return None
    return user


def create_token(email: str):
    payload = {
        "sub": email,
        "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
