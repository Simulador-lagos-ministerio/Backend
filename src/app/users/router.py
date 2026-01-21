from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.users import schemas as _schemas
from app.users import services as _services

# User auth endpoints.
router = APIRouter()


@router.post("/signup", response_model=_schemas.Token)
def signup(user: _schemas.UserCreate, db: Session = Depends(get_db)):
    if _services.get_user_by_email(db, user.email):
        raise HTTPException(status_code=400, detail="Email already registered")

    created = _services.create_user(db, user.email, user.password)
    token = _services.create_token(created.email)
    return {"access_token": token, "token_type": "bearer"}


@router.post("/signin", response_model=_schemas.Token)
def signin(user: _schemas.UserLogin, db: Session = Depends(get_db)):
    auth_user = _services.authenticate_user(db, user.email, user.password)
    if not auth_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _services.create_token(auth_user.email)
    return {"access_token": token, "token_type": "bearer"}
