"""
Integration tests for simulations endpoints.

These tests assume the following endpoints exist:

POST   /simulations
GET    /simulations
GET    /simulations/{simulation_id}
POST   /simulations/{simulation_id}/subdivisions/validate
POST   /simulations/{simulation_id}/subdivisions
DELETE /simulations/{simulation_id}/subdivisions/{subdivision_id}
POST   /simulations/{simulation_id}/finalize
POST   /simulations/{simulation_id}/unfinalize
"""
from __future__ import annotations

from uuid import UUID

import pytest


def _poly_feature_3857(minx: float, miny: float, maxx: float, maxy: float):
    """
    Build a GeoJSON Feature with a Polygon in EPSG:3857 coordinates.
    """
    return {
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [minx, maxy],
                [minx, miny],
                [maxx, miny],
                [maxx, maxy],
                [minx, maxy],
            ]],
        },
    }


def _create_simulation(client, lake_id: UUID, name: str = "Sim 1"):
    resp = client.post("/simulations", json={"name": name, "lake_id": str(lake_id)})
    assert resp.status_code in (200, 201), resp.text
    payload = resp.json()
    assert "id" in payload
    assert payload["name"] == name
    assert payload["lake_id"] == str(lake_id)
    assert payload["status"] in ("DRAFT", "FINALIZED")
    return payload


def _validate_payload(dv_id: UUID, geom_feature, all_touched: bool = False):
    return {
        "dataset_version_id": str(dv_id),
        "geometry_crs": "EPSG:3857",
        "all_touched": all_touched,
        "geometry": geom_feature,
    }


def test_create_and_list_simulations_ok(client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    sim = _create_simulation(client_postgis, lake_id)

    resp = client_postgis.get("/simulations")
    assert resp.status_code == 200, resp.text
    sims = resp.json()
    assert isinstance(sims, list)
    assert any(s["id"] == sim["id"] for s in sims)


def test_get_simulation_ok(client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    sim = _create_simulation(client_postgis, lake_id)

    resp = client_postgis.get(f"/simulations/{sim['id']}")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["id"] == sim["id"]
    assert payload["subdivisions"] == []


def test_validate_subdivision_ok_non_blocked(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    # Choose a cell that is not water (diagonal) and not inhabited (0:5,0:5 block).
    # col=6 => x 600..700, row=10 => y -1000..-900
    geom = _poly_feature_3857(600, -1000, 700, -900)

    resp = client_postgis.post(
        f"/simulations/{sim['id']}/subdivisions/validate",
        json=_validate_payload(dv_id, geom),
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["ok"] is True
    assert out["selected_cells"] > 0
    assert out["blocked_cells"] == 0
    assert isinstance(out["selection_bitset_base64"], str) and out["selection_bitset_base64"] != ""
    assert out.get("errors", []) == []


def test_validate_subdivision_intersects_water_returns_ok_false(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    # Water diagonal includes (5,5). Cell bounds: x 500..600, y -500..-400
    geom = _poly_feature_3857(500, -500, 600, -400)

    resp = client_postgis.post(
        f"/simulations/{sim['id']}/subdivisions/validate",
        json=_validate_payload(dv_id, geom),
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["ok"] is False
    # Your contract may expose specific error codes; we check presence.
    codes = {e["code"] for e in out.get("errors", [])}
    assert "INTERSECTS_WATER" in codes or out["blocked_cells"] > 0


def test_validate_subdivision_intersects_inhabitants_returns_ok_false(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    # Inhabitants block includes row 0..4, col 1 -> x 100..200, y 0..-100
    geom = _poly_feature_3857(100, -100, 200, 0)

    resp = client_postgis.post(
        f"/simulations/{sim['id']}/subdivisions/validate",
        json=_validate_payload(dv_id, geom),
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["ok"] is False
    codes = {e["code"] for e in out.get("errors", [])}
    assert "INTERSECTS_INHABITANTS" in codes or out["blocked_cells"] > 0


def test_add_subdivision_persists_geometry_and_bitset(client_postgis, postgis_session, seeded_lake, patch_s3_download_tmpcopy):
    from app.simulations.models import Subdivision  # adjust if needed

    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    geom = _poly_feature_3857(600, -1000, 700, -900)

    resp = client_postgis.post(
        f"/simulations/{sim['id']}/subdivisions",
        json=_validate_payload(dv_id, geom),
    )
    assert resp.status_code in (200, 201), resp.text
    out = resp.json()
    assert out["simulation_id"] == sim["id"]
    assert out["selected_cells"] > 0
    assert isinstance(out["selection_bitset_base64"], str) and out["selection_bitset_base64"] != ""

    # Verify persisted record in DB.
    sub_id = UUID(out["id"])
    row = postgis_session.query(Subdivision).filter(Subdivision.id == sub_id).one()

    # Tolerant geometry check: some implementations store Feature.geometry, others store the Feature itself.
    stored = row.geometry
    assert stored is not None
    if isinstance(stored, dict) and stored.get("type") == "Polygon":
        assert stored == geom["geometry"]
    else:
        assert stored == geom

    assert row.geometry_crs == "EPSG:3857"
    assert row.all_touched is False


def test_add_subdivision_overlap_returns_409(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    # First: a safe cell
    geom1 = _poly_feature_3857(600, -1000, 700, -900)
    r1 = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom1))
    assert r1.status_code in (200, 201), r1.text

    # Second: overlaps by covering the same cell (600..700,-1000..-900) plus neighbor
    geom2 = _poly_feature_3857(650, -1000, 750, -900)
    r2 = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom2))
    assert r2.status_code == 409, r2.text
    detail = r2.json().get("detail")
    assert detail in ("SUBDIVISION_OVERLAP", "CONFLICT") or isinstance(detail, dict)


def test_delete_subdivision_ok_and_allows_readd(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    geom = _poly_feature_3857(600, -1000, 700, -900)
    r1 = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom))
    assert r1.status_code in (200, 201), r1.text
    sub_id = r1.json()["id"]

    delr = client_postgis.delete(f"/simulations/{sim['id']}/subdivisions/{sub_id}")
    assert delr.status_code in (200, 204), delr.text

    # Re-add same geometry should be allowed after delete
    r2 = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom))
    assert r2.status_code in (200, 201), r2.text


def test_finalize_allowed_with_zero_subdivisions(client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    sim = _create_simulation(client_postgis, lake_id)

    resp = client_postgis.post(f"/simulations/{sim['id']}/finalize")
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["status"] == "FINALIZED"


def test_unfinalize_allows_editing_again(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    f = client_postgis.post(f"/simulations/{sim['id']}/finalize")
    assert f.status_code == 200, f.text

    u = client_postgis.post(f"/simulations/{sim['id']}/unfinalize")
    assert u.status_code == 200, u.text
    assert u.json()["status"] == "DRAFT"

    # Now adding subdivisions should work again
    geom = _poly_feature_3857(600, -1000, 700, -900)
    r = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom))
    assert r.status_code in (200, 201), r.text


def test_modifying_finalized_simulation_returns_409(client_postgis, seeded_lake, patch_s3_download_tmpcopy):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    f = client_postgis.post(f"/simulations/{sim['id']}/finalize")
    assert f.status_code == 200, f.text

    geom = _poly_feature_3857(600, -1000, 700, -900)
    r = client_postgis.post(f"/simulations/{sim['id']}/subdivisions", json=_validate_payload(dv_id, geom))
    assert r.status_code == 409, r.text
    assert r.json().get("detail") in ("SIMULATION_FINALIZED", "CONFLICT") or isinstance(r.json().get("detail"), dict)


def test_validate_multipolygon_returns_unsupported_geometry(client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    sim = _create_simulation(client_postgis, lake_id)

    multipoly = {
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [0, -100], [100, -100], [100, 0], [0, 0]]]
            ],
        },
    }

    resp = client_postgis.post(
        f"/simulations/{sim['id']}/subdivisions/validate",
        json=_validate_payload(dv_id, multipoly),
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["ok"] is False
    codes = {e["code"] for e in out.get("errors", [])}
    assert "UNSUPPORTED_GEOMETRY" in codes or "INVALID_GEOJSON" in codes
