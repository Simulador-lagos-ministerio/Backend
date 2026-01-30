# tests/unit/lakes/test_lakes_services_geometry_validation.py
"""
Unit tests for validate_and_rasterize_geometry.

We test:
- ok polygon on land
- empty selection
- water hit
- inhabitants hit
"""

from __future__ import annotations

import rasterio

from tests._helpers import find_first_cell, polygon_geojson_for_cell, rasters_dir
from tests._resolve import resolve_lakes_services


def test_validate_and_rasterize_geometry_ok(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    svc = resolve_lakes_services()
    assert hasattr(svc, "validate_and_rasterize_geometry"), "validate_and_rasterize_geometry must exist"

    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    r, c = find_first_cell(water, lambda v: v == 0.0)
    if inh[r, c] > 0:
        r, c = find_first_cell(inh, lambda v: v == 0.0)

    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r, c)

    res = svc.validate_and_rasterize_geometry(
        db,
        lake_id=lake_id,
        geometry_obj=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_id,
    )
    assert res.ok is True
    assert res.selected_cells >= 1


def test_validate_and_rasterize_geometry_empty_selection(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    svc = resolve_lakes_services()

    geom = {
        "type": "Polygon",
        "coordinates": [[[999, 999], [1000, 999], [1000, 1000], [999, 1000], [999, 999]]],
    }

    res = svc.validate_and_rasterize_geometry(
        db,
        lake_id=lake_id,
        geometry_obj=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_id,
    )
    assert res.ok is False
    assert res.selected_cells == 0


def test_validate_and_rasterize_geometry_water_hit(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    svc = resolve_lakes_services()

    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)

    r, c = find_first_cell(water, lambda v: v != 0.0)
    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r, c)

    res = svc.validate_and_rasterize_geometry(
        db,
        lake_id=lake_id,
        geometry_obj=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_id,
    )
    assert res.ok is False
    assert res.blocked_breakdown.get("water", 0) > 0


def test_validate_and_rasterize_geometry_inhabitants_hit(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    svc = resolve_lakes_services()

    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    r, c = find_first_cell(inh, lambda v: v > 0.0)
    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r, c)

    res = svc.validate_and_rasterize_geometry(
        db,
        lake_id=lake_id,
        geometry_obj=geom,
        geometry_crs=grid["crs"],
        all_touched=False,
        dataset_version_id=dv_id,
    )
    assert res.ok is False
    assert res.blocked_breakdown.get("inhabitants", 0) > 0


def test_validate_and_rasterize_geometry_invalid_geojson(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    svc = resolve_lakes_services()

    res = svc.validate_and_rasterize_geometry(
        db,
        lake_id=lake_id,
        geometry_obj={"bad": "geojson"},
        geometry_crs="EPSG:4326",
        all_touched=False,
        dataset_version_id=dv_id,
    )
    assert res.ok is False
    assert res.errors
