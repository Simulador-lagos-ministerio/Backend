from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.sqlite_database import create_sqlite_database
from app.postgis_database import create_postgis_database

from app.users.router import router as users_router
from app.lakes.router import router as lakes_router


# Application entrypoint and router wiring.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure tables exist before handling requests.
    create_sqlite_database()
    create_postgis_database()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(users_router, tags=["users"])
app.include_router(lakes_router, tags=["lakes"])
