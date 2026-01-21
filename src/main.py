from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from src import database as _database
from src import services as _services
from src import schemas as _schemas

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    _services.create_database()
    yield

app = FastAPI(lifespan=lifespan)
    
# DB dependency
def get_db():
    db = _database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/signup")
def signup(user: _schemas.UserCreate, db: Session = Depends(get_db)):
    if _services.get_user_by_email(db, user.email):
        raise HTTPException(status_code=400, detail="Email already registered")

    created = _services.create_user(db, user.email, user.password)
    token = _services.create_token(created.email)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/signin", response_model=_schemas.Token)
def signin(user: _schemas.UserLogin, db: Session = Depends(get_db)):
    auth_user = _services.authenticate_user(db, user.email, user.password)
    if not auth_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _services.create_token(auth_user.email)
    return {"access_token": token, "token_type": "bearer"}


