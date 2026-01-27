"""FastAPI application entrypoint and router wiring."""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.postgis_database import create_postgis_database
from app.sqlite_database import create_sqlite_database
from app.lakes.router import router as lakes_router
from app.users.router import router as users_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database schemas before handling requests."""
    create_sqlite_database()
    create_postgis_database()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(users_router, tags=["users"])
app.include_router(lakes_router, tags=["lakes"])
