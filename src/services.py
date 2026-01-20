from datetime import datetime, timedelta
from passlib.hash import bcrypt
from jose import jwt

from src import database as _database
from src import models as _models

# Simple JWT setup
SECRET_KEY = "dev-secret"
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60 * 24 * 60 #expira en 2 meses

# in-memory logout store
revoked_tokens = set()

def create_database():
    return _database.Base.metadata.create_all(bind=_database.engine)

# -------- USERS --------

def get_user_by_email(db, email: str):
    return db.query(_models.User).filter(_models.User.email == email).first()


def create_user(db, email: str, password: str):
    user = _models.User(
        email=email,
        hashed_password=bcrypt.hash(password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate_user(db, email: str, password: str):
    user = get_user_by_email(db, email)
    if not user:
        return None
    if not user.verify_password(password):
        return None
    return user


# -------- TOKENS --------

def create_token(email: str):
    payload = {
        "sub": email,
        "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)