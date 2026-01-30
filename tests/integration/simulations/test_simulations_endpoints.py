# tests/integration/simulations/test_simulations_endpoints.py
"""
Integration tests for simulations endpoints.

These tests assume:
- simulations endpoints require auth (Bearer token)
- geometry is validated by backend (water/inh/overlap)
- drawable errors return 200 ok=false
- hard failures return 4xx

If your simulations endpoints paths differ, adjust here (not elsewhere).
"""

from __future__ import annotations

import rasterio

from tests._helpers import find_first_cell, polygon_geojson_for_cell, rasters_dir


def test_create_simulation_list_get_flow(client, seeded_lake, auth_headers):
    lake_id, dv_id, _ = seeded_lake

    r1 = client.post(
        "/simulations",
        headers=auth_headers,
        json={"lake_id": lake_id, "name": "Sim 1", "dataset_version_id": dv_id},
    )
    assert r1.status_code == 200
    p1 = r1.json()
    assert p1["ok"] is True
    sim_id = p1["data"]["id"]

    r2 = client.get(f"/simulations/{sim_id}", headers=auth_headers)
    assert r2.status_code == 200
    p2 = r2.json()
    assert p2["ok"] is True
    assert p2["data"]["id"] == sim_id


def test_add_subdivision_overlap_and_finalize_flow(client, seeded_lake, auth_headers):
    lake_id, dv_id, grid = seeded_lake

    sim = client.post(
        "/simulations",
        headers=auth_headers,
        json={"lake_id": lake_id, "name": "Sim 2", "dataset_version_id": dv_id},
    ).json()["data"]["id"]

    # Choose a land cell (water==0 and inhab==0)
    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    r0, c0 = find_first_cell(water, lambda v: v == 0.0)
    if inh[r0, c0] > 0:
        r0, c0 = find_first_cell(inh, lambda v: v == 0.0)

    geom = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r0, c0)

    r1 = client.post(
        f"/simulations/{sim}/subdivisions",
        headers=auth_headers,
        json={
            "dataset_version_id": dv_id,
            "geometry": geom,
            "geometry_crs": grid["crs"],
            "all_touched": False,
            "inhabitants": 10,
            "impact_factor": 0.5,
        },
    )
    assert r1.status_code == 200
    p1 = r1.json()
    assert p1["ok"] is True
    sub_id = p1["data"]["id"]

    # Re-add same geom -> overlap => drawable fail (200 ok=false)
    r2 = client.post(
        f"/simulations/{sim}/subdivisions",
        headers=auth_headers,
        json={
            "dataset_version_id": dv_id,
            "geometry": geom,
            "geometry_crs": grid["crs"],
            "all_touched": False,
            "inhabitants": 5,
            "impact_factor": 0.1,
        },
    )
    assert r2.status_code == 200
    assert r2.json()["ok"] is False

    r3 = client.post(f"/simulations/{sim}/finalize", headers=auth_headers)
    assert r3.status_code == 200
    assert r3.json()["ok"] is True
    assert "id" in r3.json()["data"]

    # Delete is allowed
    r4 = client.delete(f"/simulations/{sim}/subdivisions/{sub_id}", headers=auth_headers)
    assert r4.status_code == 200
    assert r4.json()["ok"] is True


def test_create_run_and_get_by_id(client, seeded_lake, auth_headers):
    lake_id, dv_id, _ = seeded_lake

    sim_id = client.post(
        "/simulations",
        headers=auth_headers,
        json={"lake_id": lake_id, "name": "Sim Run", "dataset_version_id": dv_id},
    ).json()["data"]["id"]

    r1 = client.post(f"/simulations/{sim_id}/runs", headers=auth_headers)
    assert r1.status_code == 200
    payload = r1.json()
    assert payload["ok"] is True
    run_id = payload["data"]["id"]

    r2 = client.get(f"/runs/{run_id}", headers=auth_headers)
    assert r2.status_code == 200
    assert r2.json()["ok"] is True
