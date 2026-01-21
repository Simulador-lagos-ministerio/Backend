from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import create_database
from app.users.router import router as users_router


# Application entrypoint and router wiring.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure tables exist before handling requests.
    create_database()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(users_router, tags=["users"])
