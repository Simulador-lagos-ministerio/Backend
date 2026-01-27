import numpy as np
import pytest
from types import SimpleNamespace
from uuid import uuid4

import app.lakes.services as svc


@pytest.fixture(autouse=True)
def _clear_services_caches():
    svc._BLOCKED_CACHE.clear()
    svc._STATS_CACHE.clear()
    yield
    svc._BLOCKED_CACHE.clear()
    svc._STATS_CACHE.clear()


def _dummy_lake(rows=10, cols=10):
    # Mimics Lake model fields used by services.validate_and_rasterize_geometry
    return SimpleNamespace(
        id=uuid4(),
        grid_rows=rows,
        grid_cols=cols,
        cell_size_m=50.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
    )


def _dummy_dv():
    return SimpleNamespace(id=uuid4())


def _bool_mask(rows, cols, ones=None):
    m = np.zeros((rows, cols), dtype=bool)
    if ones:
        for (r, c) in ones:
            m[r, c] = True
    return m


# -----------------------------
# validate_and_rasterize_geometry
# -----------------------------

def test_validate_and_rasterize_lake_not_found_returns_code(monkeypatch):
    def fake_get_lake(db, lake_id):
        raise ValueError("LAKE_NOT_FOUND")

    monkeypatch.setattr(svc, "get_lake", fake_get_lake)

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=uuid4(),
        geometry_geojson={"type": "Feature", "geometry": None, "properties": {}},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "LAKE_NOT_FOUND"
    assert out["lake"] is None
    assert out["selection_mask"] is None


def test_validate_and_rasterize_dataset_not_found_returns_code(monkeypatch):
    lake = _dummy_lake()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: (_ for _ in ()).throw(ValueError("DATASET_NOT_FOUND")))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=uuid4(),
        geometry_geojson={"type": "Feature", "geometry": None, "properties": {}},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "DATASET_NOT_FOUND"
    assert out["lake"] is None
    assert out["selection_mask"] is None


def test_validate_and_rasterize_invalid_geojson_geometry_error(monkeypatch):
    lake = _dummy_lake()
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)

    # GeometryError from geometry_services
    class FakeGeometryError(Exception):
        pass

    monkeypatch.setattr(svc, "GeometryError", FakeGeometryError)

    def fake_parse(_geojson):
        raise FakeGeometryError("bad geojson")

    monkeypatch.setattr(svc, "parse_geojson_geometry", fake_parse)

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"bad": "payload"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "INVALID_GEOJSON"
    assert out["lake"] is lake
    assert out["selection_mask"] is None


def test_validate_and_rasterize_invalid_geojson_generic_exception(monkeypatch):
    lake = _dummy_lake()
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: (_ for _ in ()).throw(RuntimeError("boom")))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"x": 1},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "INVALID_GEOJSON"
    assert "GeoJSON parse error" in out["message"]
    assert out["selection_mask"] is None


def test_validate_and_rasterize_reprojection_error(monkeypatch):
    lake = _dummy_lake()
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("reproj failed")))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "INVALID_GEOMETRY"
    assert "Reprojection error" in out["message"]
    assert out["selection_mask"] is None


def test_validate_and_rasterize_empty_selection(monkeypatch):
    lake = _dummy_lake(rows=6, cols=7)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: object())

    monkeypatch.setattr(svc, "rasterize_geometry_to_mask", lambda *_args, **_kwargs: _bool_mask(6, 7, ones=None))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "EMPTY_SELECTION"
    assert out["selection_mask"] is not None
    assert int(out["selection_mask"].sum()) == 0


def test_validate_and_rasterize_layer_not_found(monkeypatch):
    lake = _dummy_lake(rows=5, cols=5)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(svc, "rasterize_geometry_to_mask", lambda *_args, **_kwargs: _bool_mask(5, 5, ones=[(0, 0)]))

    monkeypatch.setattr(svc, "get_layer", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("whatever")))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "LAYER_NOT_FOUND"
    assert out["selection_mask"] is not None


def test_validate_and_rasterize_dimension_mismatch(monkeypatch):
    lake = _dummy_lake(rows=4, cols=4)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(svc, "rasterize_geometry_to_mask", lambda *_args, **_kwargs: _bool_mask(4, 4, ones=[(0, 0), (1, 1)]))

    water_layer = SimpleNamespace()
    inh_layer = SimpleNamespace()
    monkeypatch.setattr(svc, "get_layer", lambda _db, _dv_id, kind: water_layer if kind == "water" else inh_layer)

    # wrong shapes
    monkeypatch.setattr(svc, "read_layer_array", lambda layer: np.zeros((5, 4), dtype=np.float32))

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["code"] == "DIMENSION_MISMATCH"
    assert out["selection_mask"] is not None


def test_validate_and_rasterize_ok_true_when_no_blocked(monkeypatch):
    lake = _dummy_lake(rows=3, cols=3)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: object())

    mask = _bool_mask(3, 3, ones=[(0, 0), (2, 2)])
    monkeypatch.setattr(svc, "rasterize_geometry_to_mask", lambda *_args, **_kwargs: mask)

    water_layer = SimpleNamespace()
    inh_layer = SimpleNamespace()
    monkeypatch.setattr(svc, "get_layer", lambda _db, _dv_id, kind: water_layer if kind == "water" else inh_layer)

    water = np.zeros((3, 3), dtype=np.uint8)
    inh = np.zeros((3, 3), dtype=np.float32)
    monkeypatch.setattr(svc, "read_layer_array", lambda layer: water if layer is water_layer else inh)

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is True
    assert out["selected_cells"] == 2
    assert out["blocked_cells"] == 0
    assert out["blocked_breakdown"]["water"] == 0
    assert out["blocked_breakdown"]["inhabitants"] == 0


def test_validate_and_rasterize_ok_false_when_blocked(monkeypatch):
    lake = _dummy_lake(rows=3, cols=3)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)
    monkeypatch.setattr(svc, "parse_geojson_geometry", lambda _g: object())
    monkeypatch.setattr(svc, "reproject_geometry", lambda *_args, **_kwargs: object())

    mask = _bool_mask(3, 3, ones=[(0, 0), (1, 1)])
    monkeypatch.setattr(svc, "rasterize_geometry_to_mask", lambda *_args, **_kwargs: mask)

    water_layer = SimpleNamespace()
    inh_layer = SimpleNamespace()
    monkeypatch.setattr(svc, "get_layer", lambda _db, _dv_id, kind: water_layer if kind == "water" else inh_layer)

    water = np.zeros((3, 3), dtype=np.uint8)
    water[1, 1] = 1  # water hit
    inh = np.zeros((3, 3), dtype=np.float32)
    inh[0, 0] = 10.0  # inhabitants hit
    monkeypatch.setattr(svc, "read_layer_array", lambda layer: water if layer is water_layer else inh)

    out = svc.validate_and_rasterize_geometry(
        db=None,
        lake_id=uuid4(),
        dataset_version_id=dv.id,
        geometry_geojson={"type": "Feature"},
        geometry_crs="EPSG:4326",
        all_touched=False,
    )
    assert out["ok"] is False
    assert out["selected_cells"] == 2
    assert out["blocked_cells"] == 2
    assert out["blocked_breakdown"]["water"] == 1
    assert out["blocked_breakdown"]["inhabitants"] == 1


# -----------------------------
# selection_mask_to_bitset_b64
# -----------------------------

def test_selection_mask_to_bitset_b64_uses_level_9(monkeypatch):
    called = {"level": None, "mask": None}

    def fake_mask_to_encoded_bitset(mask, level):
        called["level"] = level
        called["mask"] = mask
        return "AA=="

    monkeypatch.setattr(svc, "mask_to_encoded_bitset", fake_mask_to_encoded_bitset)

    mask = np.array([[1, 0], [0, 1]], dtype=np.uint8)
    out = svc.selection_mask_to_bitset_b64(mask)

    assert out == "AA=="
    assert called["level"] == 9
    assert called["mask"] is mask


# -----------------------------
# compute_blocked_mask (unit: cache + mismatch)
# -----------------------------

def test_compute_blocked_mask_dimension_mismatch(monkeypatch):
    lake = _dummy_lake(rows=2, cols=2)
    dv = _dummy_dv()

    monkeypatch.setattr(svc, "get_lake", lambda db, lake_id: lake)
    monkeypatch.setattr(svc, "resolve_dataset_version", lambda db, lake_id, dv_id: dv)

    water_layer = SimpleNamespace()
    inh_layer = SimpleNamespace()
    monkeypatch.setattr(svc, "get_layer", lambda _db, _dv_id, kind: water_layer if kind == "water" else inh_layer)

    monkeypatch.setattr(svc, "read_layer_array", lambda layer: np.zeros((3, 2), dtype=np.uint8))

 