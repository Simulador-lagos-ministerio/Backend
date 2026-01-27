"""Pydantic schemas for auth endpoints."""
from pydantic import BaseModel, EmailStr

# Request/response models for auth flows.
class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
