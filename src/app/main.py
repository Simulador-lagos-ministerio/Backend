from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.settings import settings
from app.postgis_database import init_db_if_configured
from app.common.errors import AppError
from app.common.responses import fail

from app.users.router import router as users_router
from app.lakes.router import router as lakes_router
from app.simulations.router import router as simulations_router


def create_app() -> FastAPI:
    app = FastAPI(title="HealthLakes Backend")

    # CORS
    origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Exception handling: unify envelope for all AppError exceptions
    @app.exception_handler(AppError)
    async def app_error_handler(_, exc: AppError) -> JSONResponse:
        return JSONResponse(
            content=fail(code=exc.code, message=exc.message, meta=exc.meta),
            status_code=exc.status_code,
        )

    # Fallback: unexpected errors (do not leak internals)
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(_, __) -> JSONResponse:
        return JSONResponse(
            content=fail(code="INTERNAL_ERROR", message="Unexpected server error."),
            status_code=500,
        )

    # Routers
    app.include_router(users_router, tags=["users"])
    app.include_router(lakes_router, tags=["lakes"])
    app.include_router(simulations_router, tags=["simulations"])

    # Optional init (disabled in production by default)
    @app.on_event("startup")
    async def _startup() -> None:
        init_db_if_configured()

    return app


app = create_app()
