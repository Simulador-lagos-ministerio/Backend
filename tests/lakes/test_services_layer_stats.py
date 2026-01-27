# tests/lakes/test_services_layer_stats.py

from __future__ import annotations

from uuid import uuid4

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from app.lakes.services import compute_layer_stats
from app.lakes.models import LakeLayer


# Ajustá estos 3 strings si tu repository usa otros códigos exactos
EXPECTED_LAKE_NOT_FOUND = "LAKE_NOT_FOUND"
EXPECTED_DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
EXPECTED_LAYER_NOT_FOUND = "LAYER_NOT_FOUND"


def _read_arr(path: str) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1)


def _expected_payload_common(lake_id, dv_id, layer_kind, rows, cols, dtype, nodata):
    return {
        "lake_id": lake_id,
        "dataset_version_id": dv_id,
        "layer_kind": layer_kind,
        "rows": rows,
        "cols": cols,
        "dtype": dtype,
        "nodata": float(nodata) if nodata is not None else None,
    }


def _data_excluding_nodata(arr: np.ndarray, nodata):
    if nodata is None:
        return arr.reshape(-1)
    return arr[arr != float(nodata)]


def test_compute_layer_stats_water_ok(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    rasters_dir = seeded_lake["rasters_dir"]

    payload = compute_layer_stats(postgis_session, lake_id, dv_id, "water")

    # DB layer metadata
    layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "WATER")
        .one()
    )

    rows, cols = 20, 20
    arr = _read_arr(str(rasters_dir / "water_ok.tif"))
    assert arr.shape == (rows, cols)

    data = _data_excluding_nodata(arr, layer.nodata)
    water_count = int((arr != 0).sum())
    expected_stats = {
        "count": int(data.size),
        "water_count": water_count,
        "water_fraction": float(water_count / (rows * cols)),
    }

    # Common fields
    common = _expected_payload_common(
        lake_id=lake_id,
        dv_id=dv_id,
        layer_kind="water",
        rows=rows,
        cols=cols,
        dtype=layer.dtype,
        nodata=layer.nodata,
    )
    for k, v in common.items():
        assert payload[k] == v

    assert payload["stats"]["count"] == expected_stats["count"]
    assert payload["stats"]["water_count"] == expected_stats["water_count"]
    assert payload["stats"]["water_fraction"] == pytest.approx(expected_stats["water_fraction"], rel=1e-9)


def test_compute_layer_stats_inhabitants_ok(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    rasters_dir = seeded_lake["rasters_dir"]

    payload = compute_layer_stats(postgis_session, lake_id, dv_id, "inhabitants")

    layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "INHABITANTS")
        .one()
    )

    rows, cols = 20, 20
    arr = _read_arr(str(rasters_dir / "inh_ok.tif"))
    assert arr.shape == (rows, cols)

    data = _data_excluding_nodata(arr, layer.nodata)
    if data.size == 0:
        expected_stats = {"count": 0}
    else:
        inhabited = int((arr > 0).sum())
        total_pop = float(np.sum(np.clip(arr, 0, None)))
        expected_stats = {
            "count": int(data.size),
            "min": float(np.min(data)),
            "max": float(np.max(data)),
            "p50": float(np.percentile(data, 50)),
            "p95": float(np.percentile(data, 95)),
            "inhabited_cells": inhabited,
            "inhabited_fraction": float(inhabited / (rows * cols)),
            "total_inhabitants": total_pop,
        }

    common = _expected_payload_common(
        lake_id=lake_id,
        dv_id=dv_id,
        layer_kind="inhabitants",
        rows=rows,
        cols=cols,
        dtype=layer.dtype,
        nodata=layer.nodata,
    )
    for k, v in common.items():
        assert payload[k] == v

    # Stats
    assert payload["stats"]["count"] == expected_stats["count"]
    if expected_stats["count"] > 0:
        assert payload["stats"]["min"] == pytest.approx(expected_stats["min"], rel=1e-9)
        assert payload["stats"]["max"] == pytest.approx(expected_stats["max"], rel=1e-9)
        assert payload["stats"]["p50"] == pytest.approx(expected_stats["p50"], rel=1e-9)
        assert payload["stats"]["p95"] == pytest.approx(expected_stats["p95"], rel=1e-9)
        assert payload["stats"]["inhabited_cells"] == expected_stats["inhabited_cells"]
        assert payload["stats"]["inhabited_fraction"] == pytest.approx(expected_stats["inhabited_fraction"], rel=1e-9)
        assert payload["stats"]["total_inhabitants"] == pytest.approx(expected_stats["total_inhabitants"], rel=1e-9)


def test_compute_layer_stats_ci_ok(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    rasters_dir = seeded_lake["rasters_dir"]

    payload = compute_layer_stats(postgis_session, lake_id, dv_id, "ci")

    layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "CI")
        .one()
    )

    rows, cols = 20, 20
    arr = _read_arr(str(rasters_dir / "ci_ok.tif"))
    assert arr.shape == (rows, cols)

    data = _data_excluding_nodata(arr, layer.nodata)
    if data.size == 0:
        expected_stats = {"count": 0}
    else:
        expected_stats = {
            "count": int(data.size),
            "min": float(np.min(data)),
            "max": float(np.max(data)),
            "p50": float(np.percentile(data, 50)),
            "p95": float(np.percentile(data, 95)),
        }

    common = _expected_payload_common(
        lake_id=lake_id,
        dv_id=dv_id,
        layer_kind="ci",
        rows=rows,
        cols=cols,
        dtype=layer.dtype,
        nodata=layer.nodata,
    )
    for k, v in common.items():
        assert payload[k] == v

    assert payload["stats"]["count"] == expected_stats["count"]
    if expected_stats["count"] > 0:
        assert payload["stats"]["min"] == pytest.approx(expected_stats["min"], rel=1e-9)
        assert payload["stats"]["max"] == pytest.approx(expected_stats["max"], rel=1e-9)
        assert payload["stats"]["p50"] == pytest.approx(expected_stats["p50"], rel=1e-9)
        assert payload["stats"]["p95"] == pytest.approx(expected_stats["p95"], rel=1e-9)


def test_compute_layer_stats_dimension_mismatch_raises(postgis_session, seeded_lake, monkeypatch, tmp_path, clear_lakes_caches):
    """
    Genera un raster 10x10 pero el lake es 20x20 => DIMENSION_MISMATCH
    """
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    # Crear raster mismatch 10x10
    out = tmp_path / "ci_mismatch.tif"
    arr = np.ones((10, 10), dtype=np.float32)
    transform = from_origin(0, 0, 1, 1)

    with rasterio.open(
        out,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype=str(arr.dtype),
        transform=transform,
        crs="EPSG:3857",
        nodata=0.0,
    ) as dst:
        dst.write(arr, 1)

    # Apuntar CI a este URI
    ci_layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "CI")
        .one()
    )
    ci_layer.storage_uri = "s3://test/ci_mismatch.tif"
    postgis_session.commit()

    def fake_download(uri: str) -> str:
        if uri.endswith("ci_mismatch.tif"):
            return str(out)
        raise FileNotFoundError(uri)

    monkeypatch.setattr("app.lakes.services.download_to_tempfile", fake_download)

    with pytest.raises(ValueError) as e:
        compute_layer_stats(postgis_session, lake_id, dv_id, "ci")
    assert str(e.value) == "DIMENSION_MISMATCH"


def test_compute_layer_stats_all_nodata_returns_count_0(postgis_session, seeded_lake, monkeypatch, tmp_path, clear_lakes_caches):
    """
    Raster 20x20 todo nodata => stats={"count":0}
    """
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    out = tmp_path / "ci_all_nodata.tif"
    nodata = 0.0
    arr = np.zeros((20, 20), dtype=np.float32)  # todo nodata
    transform = from_origin(0, 0, 1, 1)

    with rasterio.open(
        out,
        "w",
        driver="GTiff",
        height=20,
        width=20,
        count=1,
        dtype=str(arr.dtype),
        transform=transform,
        crs="EPSG:3857",
        nodata=nodata,
    ) as dst:
        dst.write(arr, 1)

    ci_layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "CI")
        .one()
    )
    ci_layer.storage_uri = "s3://test/ci_all_nodata.tif"
    ci_layer.nodata = nodata
    postgis_session.commit()

    def fake_download(uri: str) -> str:
        if uri.endswith("ci_all_nodata.tif"):
            return str(out)
        raise FileNotFoundError(uri)

    monkeypatch.setattr("app.lakes.services.download_to_tempfile", fake_download)

    payload = compute_layer_stats(postgis_session, lake_id, dv_id, "ci")
    assert payload["stats"] == {"count": 0}


def test_compute_layer_stats_nodata_none_includes_all_cells(postgis_session, seeded_lake, monkeypatch, tmp_path, clear_lakes_caches):
    """
    nodata=None => data = arr.reshape(-1), count = rows*cols
    """
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    out = tmp_path / "ci_no_nodata.tif"
    arr = np.arange(400, dtype=np.float32).reshape((20, 20))
    transform = from_origin(0, 0, 1, 1)

    with rasterio.open(
        out,
        "w",
        driver="GTiff",
        height=20,
        width=20,
        count=1,
        dtype=str(arr.dtype),
        transform=transform,
        crs="EPSG:3857",
        nodata=None,
    ) as dst:
        dst.write(arr, 1)

    ci_layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "CI")
        .one()
    )
    ci_layer.storage_uri = "s3://test/ci_no_nodata.tif"
    ci_layer.nodata = None
    postgis_session.commit()

    def fake_download(uri: str) -> str:
        if uri.endswith("ci_no_nodata.tif"):
            return str(out)
        raise FileNotFoundError(uri)

    monkeypatch.setattr("app.lakes.services.download_to_tempfile", fake_download)

    payload = compute_layer_stats(postgis_session, lake_id, dv_id, "ci")
    assert payload["stats"]["count"] == 400
    assert payload["stats"]["min"] == pytest.approx(float(np.min(arr)), rel=1e-9)
    assert payload["stats"]["max"] == pytest.approx(float(np.max(arr)), rel=1e-9)


def test_compute_layer_stats_cache_hit_does_not_redownload(postgis_session, seeded_lake, monkeypatch, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    rasters_dir = seeded_lake["rasters_dir"]

    calls = {"n": 0}

    def counting_download(uri: str) -> str:
        calls["n"] += 1
        fname = uri.split("/")[-1]
        return str(rasters_dir / fname)

    monkeypatch.setattr("app.lakes.services.download_to_tempfile", counting_download)

    p1 = compute_layer_stats(postgis_session, lake_id, dv_id, "water")
    p2 = compute_layer_stats(postgis_session, lake_id, dv_id, "water")

    assert p2 is p1  # mismo dict desde cache
    assert calls["n"] == 1  # no volvió a descargar


def test_compute_layer_stats_lake_not_found(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    dv_id = seeded_lake["dataset_version_id"]
    with pytest.raises(ValueError) as e:
        compute_layer_stats(postgis_session, uuid4(), dv_id, "water")
    assert str(e.value) == EXPECTED_LAKE_NOT_FOUND


def test_compute_layer_stats_dataset_not_found(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    lake_id = seeded_lake["lake_id"]
    with pytest.raises(ValueError) as e:
        compute_layer_stats(postgis_session, lake_id, uuid4(), "water")
    assert str(e.value) == EXPECTED_DATASET_NOT_FOUND


def test_compute_layer_stats_layer_not_found(postgis_session, seeded_lake, patch_s3_download, clear_lakes_caches):
    """
    Si tu get_layer valida y rechaza kind desconocido, esto debe explotar.
    Si en tu repo el error es otro, ajustá EXPECTED_LAYER_NOT_FOUND.
    """
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    with pytest.raises(ValueError) as e:
        compute_layer_stats(postgis_session, lake_id, dv_id, "unknown_layer_kind")
    assert str(e.value) == EXPECTED_LAYER_NOT_FOUND
