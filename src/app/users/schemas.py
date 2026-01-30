from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6, max_length=256)


class UserLogin(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6, max_length=256)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=32)


class LogoutRequest(BaseModel):
    refresh_token: str = Field(min_length=32)


class TokenPair(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    refresh_token: str
    refresh_expires_in: int  # seconds


class MeResponse(BaseModel):
    id: str
    email: EmailStr
