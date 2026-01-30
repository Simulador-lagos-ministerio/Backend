"""
Unit tests for simulations services aligned with current implementation.
"""

from __future__ import annotations

import rasterio

from uuid import UUID

from app.common.security import hash_password
from app.users.models import User
from tests._helpers import find_first_cell, polygon_geojson_for_cell, rasters_dir
from tests._resolve import resolve_simulations_services


def _make_user(db, email: str) -> User:
    user = User(email=email, hashed_password=hash_password("StrongPass123!"))
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_simulation_create_and_run(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    lake_uuid = UUID(lake_id)
    dv_uuid = UUID(dv_id)
    svc = resolve_simulations_services()

    assert hasattr(svc, "create_simulation"), "create_simulation must exist"
    assert hasattr(svc, "create_run"), "create_run must exist"

    user = _make_user(db, "user1@example.com")
    sim = svc.create_simulation(db, user_id=user.id, lake_id=lake_uuid, name="S1", dataset_version_id=dv_uuid)

    assert str(sim.lake_id) == lake_id
    assert str(sim.dataset_version_id) == dv_id

    run = svc.create_run(db, sim=sim, user_id=user.id)
    assert str(run.simulation_id) == str(sim.id)


def test_add_subdivision_rejects_water(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    lake_uuid = UUID(lake_id)
    dv_uuid = UUID(dv_id)
    svc = resolve_simulations_services()

    user = _make_user(db, "user2@example.com")
    sim = svc.create_simulation(db, user_id=user.id, lake_id=lake_uuid, name="S2", dataset_version_id=dv_uuid)

    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)

    r, c = find_first_cell(water, lambda v: v != 0.0)
    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r, c)

    sub, err = svc.add_subdivision(
        db,
        sim=sim,
        user_id=user.id,
        geometry=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_uuid,
        inhabitants=10,
        impact_factor=0.5,
    )

    assert sub is None
    assert err is not None
    assert err.get("code") == "INVALID_SELECTION"


def test_add_subdivision_rejects_overlap(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    lake_uuid = UUID(lake_id)
    dv_uuid = UUID(dv_id)
    svc = resolve_simulations_services()

    user = _make_user(db, "user3@example.com")
    sim = svc.create_simulation(db, user_id=user.id, lake_id=lake_uuid, name="S3", dataset_version_id=dv_uuid)

    # Choose a land cell (water==0 and inh==0)
    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    r0, c0 = find_first_cell(water, lambda v: v == 0.0)
    if inh[r0, c0] > 0:
        r0, c0 = find_first_cell(inh, lambda v: v == 0.0)

    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r0, c0)

    sub1, err1 = svc.add_subdivision(
        db,
        sim=sim,
        user_id=user.id,
        geometry=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_uuid,
        inhabitants=10,
        impact_factor=0.5,
    )
    assert sub1 is not None
    assert err1 is None

    sub2, err2 = svc.add_subdivision(
        db,
        sim=sim,
        user_id=user.id,
        geometry=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_uuid,
        inhabitants=5,
        impact_factor=0.2,
    )
    assert sub2 is None
    assert err2 is not None
    assert err2.get("code") == "SUBDIVISION_OVERLAP"
