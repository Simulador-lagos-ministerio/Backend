from __future__ import annotations

from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session

from app.settings import settings


class Base(DeclarativeBase):
    """Single declarative base for the entire application."""
    pass


engine = create_engine(
    settings.POSTGIS_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a DB session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db_if_configured() -> None:
    """
    Create tables only when explicitly enabled (DB_INIT_ON_STARTUP=True).

    Production best practice:
      - Use Alembic migrations instead of create_all().
    """
    if not settings.DB_INIT_ON_STARTUP:
        return

    # Import all models to ensure they are registered in Base.metadata
    # NOTE: Keep these imports inside the function to avoid import cycles.
    from app.users import models as _users_models  # noqa: F401
    from app.lakes import models as _lakes_models  # noqa: F401
    from app.simulations import models as _sim_models  # noqa: F401

    Base.metadata.create_all(bind=engine)
