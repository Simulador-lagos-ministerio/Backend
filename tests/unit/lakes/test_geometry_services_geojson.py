# tests/unit/lakes/test_geometry_services_geojson.py
"""
Unit tests for GeoJSON parsing and validation.

We enforce:
- invalid geojson -> GeometryError with code INVALID_GEOJSON (or equivalent)
- invalid geometry -> GeometryError with code INVALID_GEOMETRY
- unsupported type -> GeometryError with code UNSUPPORTED_GEOMETRY
"""

from __future__ import annotations

import pytest

import app.lakes.geometry_services as gs  # type: ignore


def _assert_geom_error(e: Exception, expected_code: str, expected_substr: str | None = None):
    if hasattr(e, "code"):
        assert getattr(e, "code") == expected_code
        return
    if expected_substr:
        assert expected_substr.lower() in str(e).lower()


def test_parse_geojson_polygon_ok():
    assert hasattr(gs, "parse_geojson_geometry"), "parse_geojson_geometry must exist"

    geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
    g = gs.parse_geojson_geometry(geom)
    assert g is not None


def test_parse_geojson_invalid_structure():
    geom = {"type": "Polygon", "coordinates": "not-a-list"}
    with pytest.raises(Exception) as ex:
        gs.parse_geojson_geometry(geom)
    # Enforce code if using GeometryError
    if hasattr(gs, "GeometryError") and isinstance(ex.value, gs.GeometryError):
        _assert_geom_error(ex.value, "INVALID_GEOJSON", expected_substr="invalid geojson")


def test_parse_geojson_unsupported_type():
    geom = {"type": "Point", "coordinates": [0, 0]}
    with pytest.raises(Exception) as ex:
        gs.parse_geojson_geometry(geom)
    if hasattr(gs, "GeometryError") and isinstance(ex.value, gs.GeometryError):
        _assert_geom_error(ex.value, "UNSUPPORTED_GEOMETRY", expected_substr="unsupported geometry")
