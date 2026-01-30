# tests/integration/lakes/test_lakes_endpoints.py
"""
Integration tests for lakes endpoints:
- GET /lakes
- GET /lakes/{id}
- GET /lakes/{id}/datasets/active
- GET /lakes/{id}/grid (or /grid-manifest fallback)
- GET /lakes/{id}/blocked-mask
- GET /lakes/{id}/datasets/{dv}/layers/{kind}/stats
- POST /lakes/{id}/validate-geometry
"""

from __future__ import annotations

import rasterio

from tests._helpers import find_first_cell, polygon_geojson_for_cell, rasters_dir


def test_get_lakes(client, seeded_lake):
    lake_id, _, _ = seeded_lake
    r = client.get("/lakes")
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is True
    assert any(x["id"] == lake_id for x in payload["data"])


def test_get_lake_detail(client, seeded_lake):
    lake_id, _, _ = seeded_lake
    r = client.get(f"/lakes/{lake_id}")
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is True
    assert payload["data"]["id"] == lake_id


def test_get_active_dataset(client, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    r = client.get(f"/lakes/{lake_id}/datasets/active")
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is True
    assert payload["data"]["id"] == dv_id


def test_get_grid(client, seeded_lake):
    lake_id, _, grid = seeded_lake
    r = client.get(f"/lakes/{lake_id}/grid")
    if r.status_code == 404:
        r = client.get(f"/lakes/{lake_id}/grid-manifest")
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is True
    spec = payload["data"].get("grid", payload["data"])
    assert spec["rows"] == grid["rows"]
    assert spec["cols"] == grid["cols"]
    assert spec["origin_corner"] == "top_left"


def test_blocked_mask_endpoint(client, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    r = client.get(f"/lakes/{lake_id}/blocked-mask", params={"dataset_version_id": dv_id})
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert "blocked_bitset_base64" in data
    assert "rows" in data and "cols" in data


def test_layer_stats_endpoints(client, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    for kind in ("water", "inhabitants", "ci"):
        r = client.get(f"/lakes/{lake_id}/datasets/{dv_id}/layers/{kind}/stats")
        assert r.status_code == 200
        payload = r.json()
        assert payload["ok"] is True
        assert "stats" in payload["data"]
        assert "count" in payload["data"]["stats"]


def test_validate_geometry_drawable_fail_is_200_ok_false(client, seeded_lake):
    lake_id, dv_id, grid = seeded_lake

    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    r0, c0 = find_first_cell(water, lambda v: v != 0.0)

    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r0, c0)

    r = client.post(
        f"/lakes/{lake_id}/validate-geometry",
        json={
            "geometry": geom,
            "geometry_crs": grid["crs"],
            "all_touched": False,
            "dataset_version_id": dv_id,
        },
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["ok"] is False
    assert payload["error"]["code"]
