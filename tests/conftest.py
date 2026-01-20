# tests/conftest.py
import pytest
import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

# ensure src is importable
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

import src.database as database
from src.main import app, get_db as app_get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def db_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,   
    )

    import src.models
    database.Base.metadata.create_all(bind=engine)

    yield engine

    database.Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(scope="function")
def client(db_engine):
    """
    FastAPI client using the in-memory DB
    """
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[app_get_db] = override_get_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()
