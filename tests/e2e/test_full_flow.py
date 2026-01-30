# tests/e2e/test_full_flow.py
"""
End-to-end flow tests:

signup -> signin -> list lakes -> create simulation -> add subdivisions -> finalize
-> reload simulation (geometry present) -> edit (delete) -> finalize again
"""

from __future__ import annotations

import rasterio

from tests._helpers import find_first_cell, polygon_geojson_for_cell, rasters_dir


def test_full_flow(client, seeded_lake):
    # signup/signin
    creds = {"email": "e2e@example.com", "password": "StrongPass123!"}
    client.post("/signup", json=creds)
    token = client.post("/signin", json=creds).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    lake_id, dv_id, grid = seeded_lake

    # list lakes
    lakes = client.get("/lakes").json()
    assert lakes["ok"] is True

    # create simulation
    sim = client.post(
        "/simulations",
        headers=headers,
        json={"lake_id": lake_id, "name": "E2E Sim", "dataset_version_id": dv_id},
    ).json()["data"]["id"]

    # pick two distinct land cells
    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    r1, c1 = find_first_cell(water, lambda v: v == 0.0)
    if inh[r1, c1] > 0:
        r1, c1 = find_first_cell(inh, lambda v: v == 0.0)

    # second land cell different
    r2, c2 = r1, c1
    for rr in range(water.shape[0]):
        for cc in range(water.shape[1]):
            if (rr, cc) != (r1, c1) and water[rr, cc] == 0 and inh[rr, cc] == 0:
                r2, c2 = rr, cc
                break
        if (r2, c2) != (r1, c1):
            break

    g1 = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r1, c1)
    g2 = polygon_geojson_for_cell(grid["origin_x"], grid["origin_y"], grid["cell_size_m"], r2, c2)

    # add subdivision 1
    sub1 = client.post(
        f"/simulations/{sim}/subdivisions",
        headers=headers,
        json={
            "dataset_version_id": dv_id,
            "geometry": g1,
            "geometry_crs": grid["crs"],
            "all_touched": False,
            "inhabitants": 10,
            "impact_factor": 0.7,
        },
    ).json()
    assert sub1["ok"] is True
    sub1_id = sub1["data"]["id"]

    # add subdivision 2
    sub2 = client.post(
        f"/simulations/{sim}/subdivisions",
        headers=headers,
        json={
            "dataset_version_id": dv_id,
            "geometry": g2,
            "geometry_crs": grid["crs"],
            "all_touched": False,
            "inhabitants": 5,
            "impact_factor": 0.2,
        },
    ).json()
    assert sub2["ok"] is True

    # finalize (alias to create a run)
    f1 = client.post(f"/simulations/{sim}/finalize", headers=headers).json()
    assert f1["ok"] is True
    assert "id" in f1["data"]

    # reload simulation and ensure geometries are present for frontend reconstruction
    sim_payload = client.get(f"/simulations/{sim}", headers=headers).json()
    assert sim_payload["ok"] is True
    assert "subdivisions" in sim_payload["data"]
    assert len(sim_payload["data"]["subdivisions"]) >= 2
    assert "geometry" in sim_payload["data"]["subdivisions"][0]

    # edit: delete one subdivision
    d = client.delete(f"/simulations/{sim}/subdivisions/{sub1_id}", headers=headers).json()
    assert d["ok"] is True

    # finalize again
    f2 = client.post(f"/simulations/{sim}/finalize", headers=headers).json()
    assert f2["ok"] is True
