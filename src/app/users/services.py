"""User auth helpers and token utilities."""
from datetime import datetime, timedelta

from jose import jwt, JWTError
from passlib.hash import bcrypt
from sqlalchemy.exc import IntegrityError
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from app.settings import settings
from app.users import models as _models
from app.sqlite_database import get_sqlite_db
from app.users.models import User

# Token settings (override via environment in production).
SECRET_KEY = settings.jwt_secret_key
ALGORITHM = settings.jwt_algorithm
TOKEN_EXPIRE_MINUTES = settings.jwt_expire_minutes

# Placeholder in-memory blacklist (not wired yet).
revoked_tokens = set()

# Dependency to get DB session.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/users/token")


def get_user_by_email(db, email: str):
    """Return user by email or None."""
    return db.query(_models.User).filter(_models.User.email == email).first()


def create_user(db, email: str, password: str):
    """Create a user with a hashed password."""
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
    """Return user when credentials are valid; otherwise None."""
    user = get_user_by_email(db, email)
    if not user:
        return None
    if not user.verify_password(password):
        return None
    return user


def create_token(email: str):
    """Create a JWT with the user email as subject."""
    payload = {
        "sub": email,
        "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme), db=Depends(get_sqlite_db)) -> User:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="INVALID_TOKEN")
    except JWTError:
        raise HTTPException(status_code=401, detail="INVALID_TOKEN")

    user = get_user_by_email(db, email)
    if not user:
        raise HTTPException(status_code=401, detail="INVALID_TOKEN")
    return user