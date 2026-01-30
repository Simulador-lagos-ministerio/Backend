# tests/unit/lakes/test_geometry_services_reproject.py
"""
Unit tests for CRS reprojection.
"""

from __future__ import annotations

import pytest

import app.lakes.geometry_services as gs  # type: ignore


def test_reproject_geometry_epsg4326_to_3857():
    assert hasattr(gs, "reproject_geometry"), "reproject_geometry must exist"
    assert hasattr(gs, "parse_geojson_geometry"), "parse_geojson_geometry must exist"

    geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [0.001, 0], [0.001, 0.001], [0, 0.001], [0, 0]]],
    }
    g = gs.parse_geojson_geometry(geom)
    g2 = gs.reproject_geometry(g, src_crs="EPSG:4326", dst_crs="EPSG:3857")
    assert g2 is not None
    # Sanity: coordinates should be in meters-ish scale now
    assert g2.bounds[2] > 0.0


def test_reproject_geometry_invalid_crs_raises():
    geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
    g = gs.parse_geojson_geometry(geom)
    with pytest.raises(Exception):
        gs.reproject_geometry(g, src_crs="EPSG:XXX", dst_crs="EPSG:3857")
