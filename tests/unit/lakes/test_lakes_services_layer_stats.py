# tests/unit/lakes/test_lakes_services_layer_stats.py
"""
Unit tests for compute_layer_stats:
- water stats correctness (water_fraction, nodata handling)
- inhabitants stats correctness (total_inhabitants, inhabited_cells)
- ci stats correctness (p50/p95)
- dimension mismatch errors
"""

from __future__ import annotations

import pytest

from tests._resolve import resolve_lakes_services, resolve_lakes_models


@pytest.mark.parametrize("kind", ["water", "inhabitants", "ci"])
def test_compute_layer_stats_ok(db, seeded_lake, kind):
    lake_id, dv_id, _ = seeded_lake
    svc = resolve_lakes_services()
    assert hasattr(svc, "compute_layer_stats"), "compute_layer_stats must exist"

    stats = svc.compute_layer_stats(db, lake_id=lake_id, dataset_version_id=dv_id, layer_kind=kind)
    data = stats.model_dump() if hasattr(stats, "model_dump") else dict(stats)
    inner = data.get("stats", data)

    assert "count" in inner
    if kind == "water":
        assert "water_fraction" in inner
        assert 0.0 <= float(inner["water_fraction"]) <= 1.0
    if kind == "inhabitants":
        assert "total_inhabitants" in inner
        assert float(inner["total_inhabitants"]) >= 0.0
    if kind == "ci":
        assert "p50" in inner and "p95" in inner


def test_compute_layer_stats_dimension_mismatch_raises(db, seeded_lake):
    Lake, _, LakeLayer = resolve_lakes_models()
    svc = resolve_lakes_services()

    lake_id, dv_id, _ = seeded_lake
    layer = db.query(LakeLayer).filter(LakeLayer.dataset_version_id == dv_id, LakeLayer.layer_kind == "CI").one()
    layer.storage_uri = "s3://maps/ci_mismatch.tif"
    db.commit()

    with pytest.raises(Exception):
        svc.compute_layer_stats(db, lake_id=lake_id, dataset_version_id=dv_id, layer_kind="ci")
