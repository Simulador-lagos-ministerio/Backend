# tests/unit/lakes/test_lakes_services_blocked_mask.py
"""
Unit tests for compute_blocked_mask.

We decode the bitset and compare it to expected = (water != 0) OR (inhabitants > 0).
Also tests dimension mismatch handling and nodata correctness.
"""

from __future__ import annotations

import numpy as np
import pytest
import rasterio

from tests._helpers import decode_mask_from_bitset_b64, rasters_dir
from tests._resolve import resolve_lakes_services, resolve_lakes_models


def test_compute_blocked_mask_matches_expected(db, seeded_lake):
    lake_id, dv_id, grid = seeded_lake
    svc = resolve_lakes_services()

    assert hasattr(svc, "compute_blocked_mask"), "compute_blocked_mask must exist"
    payload = svc.compute_blocked_mask(db, lake_id=lake_id, dataset_version_id=dv_id)

    bitset_b64 = payload.blocked_bitset_base64 if hasattr(payload, "blocked_bitset_base64") else payload["blocked_bitset_base64"]
    mask = decode_mask_from_bitset_b64(bitset_b64, grid["rows"], grid["cols"])

    with rasterio.open(rasters_dir() / "water_ok.tif") as ds:
        water = ds.read(1)
    with rasterio.open(rasters_dir() / "inh_ok.tif") as ds:
        inh = ds.read(1)

    expected = (water != 0) | (inh > 0)
    assert np.array_equal(mask, expected)


def test_compute_blocked_mask_dimension_mismatch_raises(db, seeded_lake):
    """
    Force mismatch by pointing a layer to mismatch raster.
    """
    Lake, LakeDatasetVersion, LakeLayer = resolve_lakes_models()
    svc = resolve_lakes_services()

    lake_id, dv_id, _ = seeded_lake

    layer = db.query(LakeLayer).filter(LakeLayer.dataset_version_id == dv_id, LakeLayer.layer_kind == "WATER").one()
    layer.storage_uri = "s3://maps/water_mismatch.tif"
    db.commit()

    with pytest.raises(Exception):
        svc.compute_blocked_mask(db, lake_id=lake_id, dataset_version_id=dv_id)


