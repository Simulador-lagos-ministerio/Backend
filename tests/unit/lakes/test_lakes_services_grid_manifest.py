"""
Unit tests for lakes grid manifest helpers.
"""

from __future__ import annotations

from uuid import UUID

from tests._resolve import resolve_lakes_services


def test_get_grid_manifest_shape(db, seeded_lake):
    lake_id, _, grid = seeded_lake
    svc = resolve_lakes_services()

    manifest = svc.get_grid_manifest(db, lake_id=UUID(lake_id))
    data = manifest.model_dump() if hasattr(manifest, "model_dump") else dict(manifest)

    assert str(data["lake_id"]) == lake_id
    assert data["grid"]["rows"] == grid["rows"]
    assert data["grid"]["cols"] == grid["cols"]
    assert len(data["bbox_lake_crs"]) == 4
    assert len(data["bbox_wgs84"]) == 4
    assert len(data["leaflet_bounds"]) == 2
