# tests/lakes/conftest.py
import os
import pytest
from uuid import uuid4
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

import app.sqlite_database as sqlite_database
from app.sqlite_database import get_sqlite_db
from app.main import app
import app.lakes.services as services

TESTS_DIR = Path(__file__).resolve().parents[1]     
RASTERS_DIR = TESTS_DIR / "fixtures" / "rasters"  

# -----------------------------
# SQLite (for endpoints that also touch users deps)
# -----------------------------

@pytest.fixture(scope="function")
def sqlite_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    import app.users.models
    sqlite_database.SqliteBase.metadata.create_all(bind=engine)

    yield engine

    sqlite_database.SqliteBase.metadata.drop_all(bind=engine)
    engine.dispose()


# -----------------------------
# PostGIS container + engine
# -----------------------------

@pytest.fixture(scope="session")
def postgis_container():
    image = os.getenv("POSTGIS_TEST_IMAGE", "postgis/postgis:16-3.4")
    with PostgresContainer(image=image) as pg:
        yield pg


@pytest.fixture(scope="session")
def postgis_engine(postgis_container):
    url = postgis_container.get_connection_url()
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    engine = create_engine(url, pool_pre_ping=True)
    return engine


@pytest.fixture(scope="function")
def postgis_session(postgis_engine):
    # Ensure models loaded
    import app.lakes.models  # noqa: F401
    from app.postgis_database import PostgisBase

    PostgisBase.metadata.create_all(bind=postgis_engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=postgis_engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        PostgisBase.metadata.drop_all(bind=postgis_engine)


# -----------------------------
# FastAPI client with BOTH DB overrides
# -----------------------------

@pytest.fixture(scope="function")
def client_postgis(sqlite_engine, postgis_engine):
    from app.postgis_database import get_postgis_db

    SqliteSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)
    PostgisSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=postgis_engine)

    def override_get_sqlite_db():
        db = SqliteSessionLocal()
        try:
            yield db
        finally:
            db.close()

    def override_get_postgis_db():
        db = PostgisSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_sqlite_db] = override_get_sqlite_db
    app.dependency_overrides[get_postgis_db] = override_get_postgis_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


# -----------------------------
# Seed lake + layers
# -----------------------------

@pytest.fixture(scope="function")
def seeded_lake(postgis_session):
    from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer

    base = Path(__file__).resolve().parent.parent / "fixtures" / "rasters"

    lake_id = uuid4()
    lake = Lake(
        id=lake_id,
        name="Test Lake",
        crs="EPSG:3857",
        grid_rows=20,
        grid_cols=20,
        cell_size_m=100.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        extent_geom=None,
    )
    postgis_session.add(lake)
    postgis_session.flush()

    dv = LakeDatasetVersion(
        lake_id=lake_id,
        version=1,
        status="ACTIVE",
        notes="test dataset",
    )
    postgis_session.add(dv)
    postgis_session.flush()

    layers = [
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="WATER",
            format="COG",
            storage_uri="s3://test/water_ok.tif",
            rows=20,
            cols=20,
            dtype="uint8",
            nodata=0,
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="INHABITANTS",
            format="COG",
            storage_uri="s3://test/inh_ok.tif",
            rows=20,
            cols=20,
            dtype="int32",
            nodata=0,
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="CI",
            format="COG",
            storage_uri="s3://test/ci_ok.tif",
            rows=20,
            cols=20,
            dtype="float32",
            nodata=0.0,
        ),
    ]
    postgis_session.add_all(layers)
    postgis_session.commit()

    return {
        "lake_id": lake_id,
        "dataset_version_id": dv.id,
        "rasters_dir": base,
        "uris": {
            "water": "s3://test/water_ok.tif",
            "inh": "s3://test/inh_ok.tif",
            "ci": "s3://test/ci_ok.tif",
        },
    }


# -----------------------------
# Auto patches for lakes tests
# -----------------------------

@pytest.fixture(scope="function")
def patch_s3_download(monkeypatch):
    from pathlib import Path

    rasters_dir = Path(__file__).resolve().parents[1] / "fixtures" / "rasters"

    def fake_download_to_tempfile(uri: str) -> str:
        fname = uri.split("/")[-1]
        local = rasters_dir / fname
        if not local.exists():
            raise FileNotFoundError(f"Missing test raster: {local}")
        return str(local)

    # Parcheá donde se usa
    monkeypatch.setattr("app.lakes.services.download_to_tempfile", fake_download_to_tempfile)
    # Y también repository (porque ahí importaste igual)
    monkeypatch.setattr("app.lakes.repository.download_to_tempfile", fake_download_to_tempfile)



@pytest.fixture(autouse=True)
def clear_lakes_caches():
    if hasattr(services, "_BLOCKED_CACHE"):
        services._BLOCKED_CACHE.clear()
    if hasattr(services, "_STATS_CACHE"):
        services._STATS_CACHE.clear()
    yield


@pytest.fixture(autouse=True)
def _auto_lakes_patches(patch_s3_download, clear_lakes_caches):
    # autouse: applies to every test in tests/lakes/
    yield
