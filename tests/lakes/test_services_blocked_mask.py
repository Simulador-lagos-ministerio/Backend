# tests/lakes/test_services_blocked_mask.py
import base64
import zlib
from pathlib import Path

import numpy as np
import pytest
import rasterio

from app.lakes.services import compute_blocked_mask
from app.lakes.models import LakeLayer


def _decode_zlib_base64(b64: str) -> bytes:
    return zlib.decompress(base64.b64decode(b64.encode("ascii")))


def _local_raster_path(rasters_dir: Path, uri: str) -> Path:
    return rasters_dir / uri.split("/")[-1]


def test_compute_blocked_mask_ok(postgis_session, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]
    rasters_dir = seeded_lake["rasters_dir"]

    payload = compute_blocked_mask(postgis_session, lake_id, dv_id)

    assert payload["lake_id"] == lake_id
    assert payload["dataset_version_id"] == dv_id
    assert payload["encoding"] == "bitset+zlib+base64"
    assert payload["bit_order"] == "lsb0"
    assert payload["cell_order"] == "row_major_cell_id"

    rows, cols = payload["rows"], payload["cols"]
    assert rows == 20
    assert cols == 20

    # --- expected counts desde rasters locales ---
    water_path = _local_raster_path(rasters_dir, seeded_lake["uris"]["water"])
    inh_path = _local_raster_path(rasters_dir, seeded_lake["uris"]["inh"])

    with rasterio.open(water_path) as src:
        water = src.read(1)
    with rasterio.open(inh_path) as src:
        inh = src.read(1)

    water_bool = water != 0
    inh_bool = inh > 0
    blocked = water_bool | inh_bool

    assert payload["water_count"] == int(water_bool.sum())
    assert payload["inhabited_count"] == int(inh_bool.sum())
    assert payload["blocked_count"] == int(blocked.sum())

    # --- validar bitset decodificado contra máscara blocked ---
    raw = _decode_zlib_base64(payload["blocked_bitset_base64"])

    # bytes esperados = ceil((rows*cols)/8)
    expected_len = (rows * cols + 7) // 8
    assert len(raw) == expected_len

    unpacked = np.unpackbits(np.frombuffer(raw, dtype=np.uint8), bitorder="little")[: rows * cols]
    decoded_mask = unpacked.reshape(rows, cols).astype(bool)

    assert np.array_equal(decoded_mask, blocked)


def test_compute_blocked_mask_cache_returns_same(postgis_session, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    p1 = compute_blocked_mask(postgis_session, lake_id, dv_id)
    p2 = compute_blocked_mask(postgis_session, lake_id, dv_id)

    assert p1 == p2


def test_compute_blocked_mask_dimension_mismatch(postgis_session, seeded_lake):
    lake_id = seeded_lake["lake_id"]
    dv_id = seeded_lake["dataset_version_id"]

    # Apuntamos WATER a un tif mismatch (debe existir en tests/fixtures/rasters)
    water_layer = (
        postgis_session.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dv_id)
        .filter(LakeLayer.layer_kind == "WATER")
        .one()
    )
    water_layer.storage_uri = "s3://test/water_mismatch.tif"
    postgis_session.commit()

    with pytest.raises(ValueError) as e:
        compute_blocked_mask(postgis_session, lake_id, dv_id)

    assert str(e.value) == "DIMENSION_MISMATCH"
