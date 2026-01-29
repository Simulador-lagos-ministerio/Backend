"""
Fixtures for simulations tests: PostGIS + SQLite + FastAPI client + raster fixtures.

"""
from __future__ import annotations

import os
import shutil
import importlib
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest
import rasterio
from fastapi.testclient import TestClient
from rasterio.transform import from_origin
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from testcontainers.postgres import PostgresContainer

from app.main import app
from app.sqlite_database import get_sqlite_db
from app.postgis_database import get_postgis_db

# -----------------------------
# Paths
# -----------------------------

TESTS_DIR = Path(__file__).resolve().parents[1]
RASTERS_DIR = TESTS_DIR / "fixtures" / "rasters"


# -----------------------------
# Raster fixture creation
# -----------------------------

def _write_tif(path: Path, arr: np.ndarray, *, crs: str = "EPSG:3857", nodata=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    transform = from_origin(0.0, 0.0, 1.0, 1.0)
    profile = {
        "driver": "GTiff",
        "height": int(arr.shape[0]),
        "width": int(arr.shape[1]),
        "count": 1,
        "dtype": str(arr.dtype),
        "crs": crs,
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr, 1)


def _ensure_test_rasters_exist(rows: int = 20, cols: int = 20) -> None:
    """
    Creates raster fixtures if they do not exist.
    IMPORTANT: inhabitants nodata is -9999.0 (non-zero), water nodata is 0.
    """
    # water_ok.tif: uint8, diagonal water=1, nodata=0
    p = RASTERS_DIR / "water_ok.tif"
    if not p.exists():
        arr = np.zeros((rows, cols), dtype=np.uint8)
        np.fill_diagonal(arr, 1)
        _write_tif(p, arr, nodata=0)

    # inh_ok.tif: float32, nodata=-9999.0, inhabitants block (0:5,0:5)=10
    p = RASTERS_DIR / "inh_ok.tif"
    if not p.exists():
        nodata = -9999.0
        arr = np.full((rows, cols), nodata, dtype=np.float32)
        arr[0:5, 0:5] = 10.0
        _write_tif(p, arr, nodata=nodata)

    # ci_ok.tif: float32, nodata=-9999.0 (not required for simulations but handy)
    p = RASTERS_DIR / "ci_ok.tif"
    if not p.exists():
        nodata = -9999.0
        arr = np.arange(rows * cols, dtype=np.float32).reshape(rows, cols)
        arr[0, 0] = nodata
        arr[1, 1] = nodata
        _write_tif(p, arr, nodata=nodata)


@pytest.fixture(scope="session", autouse=True)
def ensure_rasters_session() -> None:
    _ensure_test_rasters_exist(rows=20, cols=20)


# -----------------------------
# SQLite fixtures (users)
# -----------------------------

@pytest.fixture(scope="function")
def sqlite_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    import app.users.models  # noqa: F401
    import app.sqlite_database as sqlite_database
    sqlite_database.SqliteBase.metadata.create_all(bind=engine)

    yield engine

    sqlite_database.SqliteBase.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def sqlite_session(sqlite_engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -----------------------------
# PostGIS fixtures
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
    return create_engine(url, pool_pre_ping=True)


@pytest.fixture(scope="function")
def postgis_session(postgis_engine):
    import app.lakes.models  # noqa: F401
    import app.simulations.models  # noqa: F401
    from app.postgis_database import PostgisBase

    PostgisBase.metadata.create_all(bind=postgis_engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=postgis_engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        PostgisBase.metadata.drop_all(bind=postgis_engine)


# -----------------------------
# Auth override helper
# -----------------------------

@dataclass
class DummyUser:
    id: int
    email: str


def _override_current_user_dependency(user: DummyUser) -> None:
    """
    Try to override whichever dependency your simulations router uses for auth.

    This is resilient: it attempts multiple common patterns.
    """
    candidates = [
        ("app.simulations.router", "get_current_user"),
        ("app.simulations.router", "get_current_user_email"),
        ("app.users.dependencies", "get_current_user"),
        ("app.users.dependencies", "get_current_user_email"),
        ("app.users.router", "get_current_user"),
        ("app.users.router", "get_current_user_email"),
    ]

    for mod_name, fn_name in candidates:
        try:
            mod = importlib.import_module(mod_name)
            dep = getattr(mod, fn_name)
        except Exception:
            continue

        # Override with the minimal object or email depending on expected signature.
        if "email" in fn_name:
            app.dependency_overrides[dep] = lambda: user.email
        else:
            app.dependency_overrides[dep] = lambda: user
        return

    # If your project uses another dependency name, add it to candidates.
    # We intentionally do not raise here to allow service-only tests.


# -----------------------------
# FastAPI client
# -----------------------------

@pytest.fixture(scope="function")
def client_postgis(sqlite_engine, postgis_engine):
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
# Seed user
# -----------------------------

@pytest.fixture(scope="function")
def seeded_user(sqlite_session):
    from app.users import models as user_models

    user = user_models.User(
        email="tester@example.com",
        hashed_password="not-used-in-tests",
    )
    sqlite_session.add(user)
    sqlite_session.commit()
    sqlite_session.refresh(user)
    return DummyUser(id=int(user.id), email=str(user.email))


@pytest.fixture(scope="function", autouse=True)
def override_auth(seeded_user):
    _override_current_user_dependency(seeded_user)
    yield
    # Do not clear globally here; client fixture clears at end.


# -----------------------------
# Seed lake + dataset + layers (water/inhabitants)
# -----------------------------

@pytest.fixture(scope="function")
def seeded_lake(postgis_session):
    from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer

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
            dtype="float32",
            nodata=-9999.0,
        ),
    ]
    postgis_session.add_all(layers)
    postgis_session.commit()

    return {"lake_id": lake_id, "dataset_version_id": dv.id}


# -----------------------------
# Patch S3 downloads to temp copies (so cleanup won't delete fixtures)
# -----------------------------

@pytest.fixture(scope="function")
def patch_s3_download_tmpcopy(monkeypatch, tmp_path_factory):
    """
    Patch the S3 download helper used by read_layer_array so it returns a temp copy.
    This ensures remove_tempfile() deletes only temporary files, not fixture rasters.
    """
    temp_root = tmp_path_factory.mktemp("s3_tmp_copies")

    def fake_download_to_tempfile(uri: str) -> str:
        fname = uri.split("/")[-1]
        src = RASTERS_DIR / fname
        if not src.exists():
            raise FileNotFoundError(f"Missing raster fixture: {src}")
        dst = temp_root / f"{uuid4()}_{fname}"
        shutil.copy(src, dst)
        return str(dst)

    # Patch call sites.
    monkeypatch.setattr("app.lakes.repository.download_to_tempfile", fake_download_to_tempfile, raising=True)
    monkeypatch.setattr("app.storage.s3_client.download_to_tempfile", fake_download_to_tempfile, raising=True)

    yield
