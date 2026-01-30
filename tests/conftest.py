# tests/conftest.py
"""
Global pytest fixtures:

- Start PostGIS Postgres via testcontainers (session-scoped).
- Create schema via Base.metadata.create_all/drop_all (until Alembic).
- Patch S3 downloads to local fixture rasters (no MinIO needed).
- Seed a lake + ACTIVE dataset + layers (water/inhabitants/ci).
- Provide FastAPI TestClient with DB dependency override.
- Provide auth helpers for users API (/signup, /signin) without breaking frontend contract.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Generator, Tuple

import pytest
import rasterio
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer

from tests._helpers import rasters_dir
from tests._resolve import (
    resolve_app,
    resolve_db_symbols,
    resolve_lakes_models,
)

# ----------------------------
# Environment defaults for settings import safety
# ----------------------------

os.environ.setdefault("SKIP_DB_INIT", "1")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")
os.environ.setdefault("JWT_ALGORITHM", "HS256")
os.environ.setdefault("JWT_EXPIRE_MINUTES", "30")
os.environ.setdefault("CORS_ORIGINS", "*")

# These S3 envs are placeholders; we patch S3 access in tests.
os.environ.setdefault("S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("S3_ACCESS_KEY", "minio")
os.environ.setdefault("S3_SECRET_KEY", "minio123")
os.environ.setdefault("S3_BUCKET", "maps")


# ----------------------------
# Raster fixtures generation
# ----------------------------

@pytest.fixture(scope="session", autouse=True)
def ensure_fixture_rasters() -> None:
    """
    Ensure fixture rasters exist by executing make_test_rasters.py (as requested).
    """
    target = rasters_dir() / "water_ok.tif"
    if target.exists():
        return

    # Import and run generator.
    from tests.fixtures.make_test_rasters import main as make_rasters  # type: ignore
    make_rasters()

    if not target.exists():
        raise RuntimeError("Raster fixtures were not generated as expected.")


# ----------------------------
# PostGIS container / SQLAlchemy engine
# ----------------------------

@pytest.fixture(scope="session")
def postgis_container() -> Generator[PostgresContainer, None, None]:
    """
    Start a PostGIS-enabled Postgres container for the whole test session.
    """
    container = PostgresContainer("postgis/postgis:15-3.4")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgis_engine(postgis_container: PostgresContainer):
    """
    Session-wide SQLAlchemy engine for tests.
    """
    url = postgis_container.get_connection_url()
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    os.environ["POSTGIS_URL"] = url

    engine = create_engine(url, pool_pre_ping=True)

    # Ensure PostGIS extension exists.
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.commit()

    return engine


@pytest.fixture(scope="session")
def SessionLocal(postgis_engine):
    """
    Session factory bound to the test engine.
    """
    return sessionmaker(autocommit=False, autoflush=False, bind=postgis_engine)


@pytest.fixture(scope="session", autouse=True)
def create_schema(postgis_engine):
    """
    Create and drop schema once per test run (until Alembic is integrated).
    """
    Base, _ = resolve_db_symbols()

    # Import models so metadata is populated.
    import app.users.models  # noqa: F401
    import app.lakes.models  # noqa: F401
    import app.simulations.models  # noqa: F401

    Base.metadata.create_all(bind=postgis_engine)
    yield
    Base.metadata.drop_all(bind=postgis_engine)


@pytest.fixture()
def db(SessionLocal, postgis_engine) -> Generator[Session, None, None]:
    """
    Function-scoped DB session with nested transaction so commits in code
    don't leak data across tests.
    """
    connection = postgis_engine.connect()
    transaction = connection.begin()
    session = SessionLocal(bind=connection)
    session.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def _restart_savepoint(sess, trans) -> None:  # type: ignore[no-redef]
        if trans.nested and not sess.in_nested_transaction():
            sess.begin_nested()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


# ----------------------------
# S3 patching (fixture rasters instead of MinIO)
# ----------------------------

@pytest.fixture()
def patch_s3_download(monkeypatch) -> Callable[[str], str]:
    """
    Patch download_to_tempfile(uri) to map s3://.../file.tif to local fixture rasters.

    Returns the patched function for optional direct use.
    """
    fixtures = rasters_dir()

    def fake_download_to_tempfile(uri: str) -> str:
        if not uri.startswith("s3://"):
            raise ValueError(f"Unsupported URI in tests: {uri}")

        filename = uri.split("/")[-1]
        src = fixtures / filename
        if not src.exists():
            raise FileNotFoundError(f"Missing raster fixture: {src}")

        fd, dst = tempfile.mkstemp(suffix=src.suffix)
        os.close(fd)
        shutil.copyfile(src, dst)
        return dst

    # Patch canonical module
    import app.storage.s3_client as s3_client  # type: ignore
    monkeypatch.setattr(s3_client, "download_to_tempfile", fake_download_to_tempfile, raising=True)

    # Patch potential direct-import call sites
    for mod_path in ("app.lakes.repository", "app.lakes.services"):
        try:
            mod = __import__(mod_path, fromlist=["download_to_tempfile"])
            monkeypatch.setattr(mod, "download_to_tempfile", fake_download_to_tempfile, raising=False)
        except Exception:
            pass

    return fake_download_to_tempfile


# ----------------------------
# Seed: lake + active dataset + layers
# ----------------------------

@pytest.fixture()
def seeded_lake(db: Session, patch_s3_download) -> Tuple[str, str, dict]:
    """
    Insert:
    - 1 lake
    - 1 ACTIVE dataset version
    - layers: WATER, INHABITANTS, CI

    Storage URIs point to s3://maps/<fixture>.tif (patched to local files).

    Returns: (lake_id, dataset_version_id, grid_spec)
    """
    Lake, LakeDatasetVersion, LakeLayer = resolve_lakes_models()

    water_path = rasters_dir() / "water_ok.tif"
    inh_path = rasters_dir() / "inh_ok.tif"
    ci_path = rasters_dir() / "ci_ok.tif"

    with rasterio.open(water_path) as ds:
        rows, cols = ds.height, ds.width
        nodata_water = ds.nodata if ds.nodata is not None else 0
        dtype_water = ds.dtypes[0]

    with rasterio.open(inh_path) as ds:
        nodata_inh = ds.nodata if ds.nodata is not None else 0
        dtype_inh = ds.dtypes[0]

    with rasterio.open(ci_path) as ds:
        nodata_ci = ds.nodata if ds.nodata is not None else 0.0
        dtype_ci = ds.dtypes[0]

    lake = Lake(
        name="Test Lake",
        crs="EPSG:3857",
        grid_rows=rows,
        grid_cols=cols,
        cell_size_m=1.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        created_at=datetime.now(timezone.utc) if hasattr(Lake, "created_at") else None,
    )
    db.add(lake)
    db.flush()

    dv = LakeDatasetVersion(
        lake_id=lake.id,
        status="ACTIVE",
        created_at=datetime.now(timezone.utc) if hasattr(LakeDatasetVersion, "created_at") else None,
    )
    db.add(dv)
    db.flush()

    layers = [
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="WATER",
            storage_uri="s3://maps/water_ok.tif",
            rows=rows,
            cols=cols,
            dtype=str(dtype_water),
            nodata=None if float(nodata_water) == 0.0 else float(nodata_water),
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="INHABITANTS",
            storage_uri="s3://maps/inh_ok.tif",
            rows=rows,
            cols=cols,
            dtype=str(dtype_inh),
            nodata=None if float(nodata_inh) == 0.0 else float(nodata_inh),
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="CI",
            storage_uri="s3://maps/ci_ok.tif",
            rows=rows,
            cols=cols,
            dtype=str(dtype_ci),
            nodata=None if float(nodata_ci) == 0.0 else float(nodata_ci),
        ),
    ]
    db.add_all(layers)
    db.commit()

    grid = {
        "rows": rows,
        "cols": cols,
        "cell_size_m": 1.0,
        "crs": "EPSG:3857",
        "origin_corner": "top_left",
        "origin_x": 0.0,
        "origin_y": 0.0,
    }
    return str(lake.id), str(dv.id), grid


# ----------------------------
# FastAPI TestClient with DB override
# ----------------------------

@pytest.fixture()
def client(db: Session) -> Generator[TestClient, None, None]:
    """
    TestClient bound to function-scoped DB session via dependency override.
    """
    Base, get_db = resolve_db_symbols()
    app = resolve_app()

    def _override_db():
        yield db

    app.dependency_overrides[get_db] = _override_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


# ----------------------------
# Auth helpers (users endpoints must remain compatible)
# ----------------------------

@pytest.fixture()
def user_token(client: TestClient) -> str:
    """
    Create user via /signup then /signin, returning an access token.
    """
    payload = {"email": "user1@example.com", "password": "StrongPass123!"}

    r1 = client.post("/signup", json=payload)
    assert r1.status_code in (200, 201, 400)

    r2 = client.post("/signin", json=payload)
    assert r2.status_code == 200
    data = r2.json()
    assert "access_token" in data
    return data["access_token"]


@pytest.fixture()
def auth_headers(user_token: str) -> dict:
    """
    Authorization header for protected endpoints.
    """
    return {"Authorization": f"Bearer {user_token}"}


@pytest.fixture()
def user2_token(client: TestClient) -> str:
    payload = {"email": "user2@example.com", "password": "StrongPass123!"}
    client.post("/signup", json=payload)
    r = client.post("/signin", json=payload)
    assert r.status_code == 200
    return r.json()["access_token"]


@pytest.fixture()
def auth_headers2(user2_token: str) -> dict:
    return {"Authorization": f"Bearer {user2_token}"}


# ----------------------------
# Cache cleanup (optional)
# ----------------------------

@pytest.fixture(autouse=True)
def clear_lakes_caches():
    """
    If lakes services uses module-level caches (TTLCache), clear them between tests.
    This avoids cross-test contamination.
    """
    try:
        import app.lakes.services as s  # type: ignore
        for name in ("_STATS_CACHE", "_BLOCKED_CACHE", "_BLOCKED_MASK_CACHE"):
            cache = getattr(s, name, None)
            if cache is not None and hasattr(cache, "clear"):
                cache.clear()
    except Exception:
        pass
    yield
