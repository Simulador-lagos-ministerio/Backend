# tests/lakes/test_router.py

from __future__ import annotations

from uuid import UUID, uuid4

import numpy as np
import pytest
from pyproj import Transformer

from app.lakes.models import Lake, LakeDatasetVersion
from app.lakes.schemas import (
    BlockedMaskResponse,
    DatasetVersionSummary,
    GeometryValidationResponse,
    GridManifest,
    LakeDetail,
    LakeSummary,
    LayerStats,
    RasterizeResponse,
)

# -----------------------
# Helpers
# -----------------------

def _validate_list(model_cls, payload_list):
    return [model_cls.model_validate(x) for x in payload_list]


def _bbox_manual_top_left(origin_x: float, origin_y: float, cols: int, rows: int, cell_size: float):
    """
    For origin_corner="top_left" and Y decreasing downward:
    minx = origin_x
    maxx = origin_x + cols*cell
    maxy = origin_y
    miny = origin_y - rows*cell
    """
    minx = origin_x
    maxx = origin_x + cols * cell_size
    maxy = origin_y
    miny = origin_y - rows * cell_size
    return (minx, miny, maxx, maxy)


def _geom_payload(dv_id: UUID):
    return {
        "dataset_version_id": str(dv_id),
        "geometry_crs": "EPSG:4326",
        "all_touched": False,
        "geometry": {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 0.001], [0.001, 0.001], [0.001, 0], [0, 0]]],
            },
        },
    }


# -----------------------
# /lakes
# -----------------------

def test_list_lakes_empty(postgis_session, client_postgis):
    resp = client_postgis.get("/lakes")
    assert resp.status_code == 200
    items = resp.json()
    assert items == []


def test_list_lakes_includes_active_dataset(postgis_session, client_postgis, seeded_lake):
    resp = client_postgis.get("/lakes")
    assert resp.status_code == 200

    lakes = _validate_list(LakeSummary, resp.json())
    assert len(lakes) == 1

    lk = lakes[0]
    assert lk.id == seeded_lake["lake_id"]
    assert lk.active_dataset_version_id == seeded_lake["dataset_version_id"]
    assert lk.grid.rows == 20
    assert lk.grid.cols == 20
    assert lk.grid.cell_size_m == 100.0
    assert lk.grid.crs == "EPSG:3857"


def test_list_lakes_active_dataset_none_when_no_active(postgis_session, client_postgis):
    # Create a lake without ACTIVE dataset version
    lake_id = uuid4()
    lake = Lake(
        id=lake_id,
        name="No Active Lake",
        crs="EPSG:3857",
        grid_rows=10,
        grid_cols=10,
        cell_size_m=50.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        extent_geom=None,
    )
    postgis_session.add(lake)
    postgis_session.commit()

    resp = client_postgis.get("/lakes")
    assert resp.status_code == 200
    lakes = _validate_list(LakeSummary, resp.json())
    assert len(lakes) == 1

    assert lakes[0].id == lake_id
    assert lakes[0].active_dataset_version_id is None


# -----------------------
# /lakes/{lake_id}
# -----------------------

def test_get_lake_ok(postgis_session, client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    resp = client_postgis.get(f"/lakes/{lake_id}")
    assert resp.status_code == 200

    payload = LakeDetail.model_validate(resp.json())
    assert payload.id == lake_id
    assert payload.active_dataset_version_id == seeded_lake["dataset_version_id"]
    assert payload.grid.rows == 20
    assert payload.grid.cols == 20
    assert payload.extent_bbox is None


def test_get_lake_404(postgis_session, client_postgis):
    resp = client_postgis.get(f"/lakes/{uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lake not found"


# -----------------------
# /lakes/{lake_id}/datasets/active
# -----------------------

def test_get_active_dataset_ok(postgis_session, client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    resp = client_postgis.get(f"/lakes/{lake_id}/datasets/active")
    assert resp.status_code == 200

    payload = DatasetVersionSummary.model_validate(resp.json())
    assert payload.id == seeded_lake["dataset_version_id"]
    assert payload.lake_id == lake_id
    assert payload.status == "ACTIVE"
    assert payload.version == 1


def test_get_active_dataset_lake_not_found_404(postgis_session, client_postgis):
    resp = client_postgis.get(f"/lakes/{uuid4()}/datasets/active")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lake not found"


def test_get_active_dataset_dataset_not_found_404(postgis_session, client_postgis):
    lake_id = uuid4()

    lake = Lake(
        id=lake_id,
        name="No active DV",
        crs="EPSG:3857",
        grid_rows=10,
        grid_cols=10,
        cell_size_m=50.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        extent_geom=None,
    )
    postgis_session.add(lake)
    postgis_session.commit()

    resp = client_postgis.get(f"/lakes/{lake_id}/datasets/active")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Dataset not found"


# -----------------------
# /lakes/{lake_id}/blocked-mask
# -----------------------

def test_get_blocked_mask_ok(postgis_session, client_postgis, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]

    resp = client_postgis.get(f"/lakes/{lake_id}/blocked-mask")
    assert resp.status_code == 200

    payload = BlockedMaskResponse.model_validate(resp.json())
    assert payload.lake_id == lake_id
    assert payload.dataset_version_id == seeded_lake["dataset_version_id"]
    assert payload.rows == 20
    assert payload.cols == 20

    # contract fields
    assert payload.encoding == "bitset+zlib+base64"
    assert payload.bit_order == "lsb0"
    assert payload.cell_order == "row_major_cell_id"
    assert isinstance(payload.blocked_bitset_base64, str)
    assert payload.blocked_bitset_base64 != ""

    # optional counts (if service populates them)
    if payload.blocked_count is not None:
        assert payload.blocked_count >= 0
    if payload.water_count is not None:
        assert payload.water_count >= 0
    if payload.inhabited_count is not None:
        assert payload.inhabited_count >= 0



def test_get_blocked_mask_lake_not_found_404(postgis_session, client_postgis, patch_s3_download, clear_lakes_caches):
    resp = client_postgis.get(f"/lakes/{uuid4()}/blocked-mask")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lake not found"


def test_get_blocked_mask_dataset_not_found_404(postgis_session, client_postgis, patch_s3_download, clear_lakes_caches):
    # lake exists but no ACTIVE dataset
    lake_id = uuid4()
    lake = Lake(
        id=lake_id,
        name="No active dataset",
        crs="EPSG:3857",
        grid_rows=10,
        grid_cols=10,
        cell_size_m=50.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        extent_geom=None,
    )
    postgis_session.add(lake)
    postgis_session.commit()

    resp = client_postgis.get(f"/lakes/{lake_id}/blocked-mask")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Dataset not found"


# -----------------------
# /lakes/{lake_id}/datasets/{dv}/layers/{layer_kind}/stats
# -----------------------

@pytest.mark.parametrize("layer_kind", ["water", "inhabitants", "ci"])
def test_layer_stats_ok(postgis_session, client_postgis, seeded_lake, patch_s3_download, clear_lakes_caches, layer_kind):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    resp = client_postgis.get(f"/lakes/{lake_id}/datasets/{dv_id}/layers/{layer_kind}/stats")
    assert resp.status_code == 200

    payload = LayerStats.model_validate(resp.json())
    assert payload.lake_id == lake_id
    assert payload.dataset_version_id == dv_id
    assert payload.layer_kind == layer_kind
    assert payload.rows == 20
    assert payload.cols == 20
    assert "count" in payload.stats


def test_layer_stats_lake_not_found_404(postgis_session, client_postgis, seeded_lake, patch_s3_download, clear_lakes_caches):
    dv_id = seeded_lake["dataset_version_id"]
    resp = client_postgis.get(f"/lakes/{uuid4()}/datasets/{dv_id}/layers/water/stats")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lake not found"


def test_layer_stats_dataset_not_found_404(postgis_session, client_postgis, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    resp = client_postgis.get(f"/lakes/{lake_id}/datasets/{uuid4()}/layers/water/stats")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Dataset not found"


def test_layer_stats_layer_not_found_404(postgis_session, client_postgis, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    resp = client_postgis.get(f"/lakes/{lake_id}/datasets/{dv_id}/layers/not_a_layer/stats")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Layer not found"


# -----------------------
# /lakes/{lake_id}/grid
# -----------------------

def test_get_grid_manifest_ok(postgis_session, client_postgis, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    resp = client_postgis.get(f"/lakes/{lake_id}/grid")
    assert resp.status_code == 200

    payload = GridManifest.model_validate(resp.json())
    assert payload.lake_id == lake_id
    assert payload.grid.rows == 20
    assert payload.grid.cols == 20
    assert payload.grid.cell_size_m == 100.0
    assert payload.grid.crs == "EPSG:3857"

    # Validate bbox_mercator against manual math (top_left origin in seed)
    expected = _bbox_manual_top_left(0.0, 0.0, cols=20, rows=20, cell_size=100.0)
    assert payload.bbox_mercator == pytest.approx([expected[0], expected[1], expected[2], expected[3]], rel=1e-12)

    # bbox_wgs84: independently transform mercator corners to EPSG:4326
    t = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    minlon, minlat = t.transform(expected[0], expected[1])
    maxlon, maxlat = t.transform(expected[2], expected[3])
    assert payload.bbox_wgs84 == pytest.approx([minlon, minlat, maxlon, maxlat], rel=1e-9)


def test_get_grid_manifest_404(postgis_session, client_postgis):
    resp = client_postgis.get(f"/lakes/{uuid4()}/grid")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lake not found"


# -----------------------
# /lakes/{lake_id}/validate-geometry (monkeypatch contract)
# -----------------------

def test_validate_geometry_ok_no_blocked(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    # Patch router-local symbol (important)
    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.zeros((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        mask[0, 0] = 1
        return {
            "ok": True,
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
            "selected_cells": int(mask.sum()),
            "blocked_cells": 0,
            "blocked_breakdown": {"water": 0, "inhabitants": 0},
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)
    monkeypatch.setattr("app.lakes.router.selection_mask_to_bitset_b64", lambda _m: "AA==")

    resp = client_postgis.post(f"/lakes/{lake_id}/validate-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200

    payload = GeometryValidationResponse.model_validate(resp.json())
    assert payload.ok is True
    assert payload.lake_id == lake_id
    assert payload.dataset_version_id == dv_id
    assert payload.rows == 20
    assert payload.cols == 20
    assert payload.selected_cells == 1
    assert payload.blocked_cells == 0
    assert payload.blocked_breakdown == {"water": 0, "inhabitants": 0}
    assert payload.selection_bitset_base64 == "AA=="
    assert payload.errors == []


def test_validate_geometry_ok_but_blocked_adds_errors(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.ones((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        return {
            "ok": False,
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
            "selected_cells": int(mask.sum()),
            "blocked_cells": 10,
            "blocked_breakdown": {"water": 5, "inhabitants": 5},
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)
    monkeypatch.setattr("app.lakes.router.selection_mask_to_bitset_b64", lambda _m: "AA==")

    resp = client_postgis.post(f"/lakes/{lake_id}/validate-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200

    payload = GeometryValidationResponse.model_validate(resp.json())
    assert payload.ok is False
    assert payload.blocked_cells == 10
    assert payload.blocked_breakdown["water"] == 5
    assert payload.blocked_breakdown["inhabitants"] == 5
    # Should include both errors
    codes = {e.code for e in payload.errors}
    assert "INTERSECTS_WATER" in codes
    assert "INTERSECTS_INHABITANTS" in codes


def test_validate_geometry_empty_selection_adds_error(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.zeros((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        return {
            "ok": False,
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
            "selected_cells": 0,
            "blocked_cells": 0,
            "blocked_breakdown": {"water": 0, "inhabitants": 0},
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)
    monkeypatch.setattr("app.lakes.router.selection_mask_to_bitset_b64", lambda _m: None)

    resp = client_postgis.post(f"/lakes/{lake_id}/validate-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200
    payload = GeometryValidationResponse.model_validate(resp.json())
    codes = {e.code for e in payload.errors}
    assert "EMPTY_SELECTION" in codes


def test_validate_geometry_service_error_lake_none_minimal(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = uuid4()
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        return {"code": "LAKE_NOT_FOUND", "message": "Lake not found", "lake": None, "dataset_version_id": dataset_version_id}

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)

    resp = client_postgis.post(f"/lakes/{lake_id}/validate-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200

    payload = GeometryValidationResponse.model_validate(resp.json())
    assert payload.ok is False
    assert payload.lake_id == lake_id
    assert payload.dataset_version_id == dv_id
    assert payload.rows == 0
    assert payload.cols == 0
    assert len(payload.errors) == 1
    assert payload.errors[0].code == "LAKE_NOT_FOUND"


def test_validate_geometry_service_error_with_lake_and_selection(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.zeros((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        mask[0, 0] = 1
        return {
            "code": "INVALID_GEOMETRY",
            "message": "bad geometry",
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)
    monkeypatch.setattr("app.lakes.router.selection_mask_to_bitset_b64", lambda _m: "AA==")

    resp = client_postgis.post(f"/lakes/{lake_id}/validate-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200
    payload = GeometryValidationResponse.model_validate(resp.json())
    assert payload.ok is False
    assert payload.rows == 20
    assert payload.cols == 20
    assert payload.selected_cells == 1
    assert payload.selection_bitset_base64 == "AA=="
    assert payload.errors[0].code == "INVALID_GEOMETRY"


# -----------------------
# /lakes/{lake_id}/rasterize-geometry (monkeypatch contract)
# -----------------------

def test_rasterize_geometry_ok(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.zeros((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        mask[0, 0] = 1
        return {
            "ok": True,
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
            "selected_cells": 1,
            "blocked_cells": 0,
            "blocked_breakdown": {"water": 0, "inhabitants": 0},
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)
    monkeypatch.setattr("app.lakes.router.selection_mask_to_bitset_b64", lambda _m: "AA==")

    resp = client_postgis.post(f"/lakes/{lake_id}/rasterize-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 200
    payload = RasterizeResponse.model_validate(resp.json())
    assert payload.lake_id == lake_id
    assert payload.dataset_version_id == dv_id
    assert payload.rows == 20
    assert payload.cols == 20
    assert payload.cell_count == 1
    assert payload.selection_bitset_base64 == "AA=="


def test_rasterize_geometry_service_code_returns_400(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        return {"code": "INVALID_GEOMETRY", "message": "bad geometry"}

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)

    resp = client_postgis.post(f"/lakes/{lake_id}/rasterize-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["code"] == "INVALID_GEOMETRY"
    assert "message" in detail


def test_rasterize_INVALID_GEOMETRY_selection_returns_400(postgis_session, client_postgis, seeded_lake, monkeypatch):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    def fake_validate_and_rasterize(*, db, lake_id, dataset_version_id, geometry_geojson, geometry_crs, all_touched):
        lake = db.query(Lake).filter(Lake.id == lake_id).one()
        mask = np.ones((int(lake.grid_rows), int(lake.grid_cols)), dtype=np.uint8)
        return {
            "ok": False,
            "lake": lake,
            "dataset_version_id": dataset_version_id,
            "selection_mask": mask,
            "selected_cells": int(mask.sum()),
            "blocked_cells": 7,
            "blocked_breakdown": {"water": 7, "inhabitants": 0},
        }

    monkeypatch.setattr("app.lakes.router.validate_and_rasterize_geometry", fake_validate_and_rasterize)

    resp = client_postgis.post(f"/lakes/{lake_id}/rasterize-geometry", json=_geom_payload(dv_id))
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["code"] == "INVALID_SELECTION"
