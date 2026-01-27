# tests/users/conftest.py
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

import app.sqlite_database as sqlite_database
from app.sqlite_database import get_sqlite_db
from app.main import app


@pytest.fixture(scope="function")
def db_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    import app.users.models  # asegura metadata
    sqlite_database.SqliteBase.metadata.create_all(bind=engine)

    yield engine

    sqlite_database.SqliteBase.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="function")
def client(db_engine):
    """
    FastAPI client using the in-memory SQLite DB (users).
    """
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

    def override_get_sqlite_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_sqlite_db] = override_get_sqlite_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()
