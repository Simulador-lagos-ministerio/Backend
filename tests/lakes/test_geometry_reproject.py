"""Unit tests for geometry reprojection."""
import pytest
from shapely.geometry import Polygon

from app.lakes.geometry_services import reproject_geometry


def _square_wgs84():
    # 0.01° x 0.01° square near the origin.
    return Polygon([
        (0.0, 0.0),
        (0.0, 0.01),
        (0.01, 0.01),
        (0.01, 0.0),
        (0.0, 0.0),
    ])


def test_reproject_identity_returns_same_geometry():
    geom = _square_wgs84()
    out = reproject_geometry(geom, "EPSG:4326", "EPSG:4326")

    assert out.equals_exact(geom, tolerance=0.0)
    assert out.is_valid is True
    assert out.is_empty is False


def test_reproject_wgs84_to_mercator_changes_coordinates():
    geom = _square_wgs84()
    out = reproject_geometry(geom, "EPSG:4326", "EPSG:3857")

    assert out.is_empty is False
    assert out.is_valid is True

    # Coordinates should change (degrees -> meters).
    assert out.equals_exact(geom, tolerance=0.0) is False

    # Near the origin, (0,0) remains close to (0,0) in Mercator.
    x0, y0 = list(out.exterior.coords)[0]
    assert abs(x0) < 1e-6
    assert abs(y0) < 1e-6


def test_reproject_roundtrip_wgs84_to_mercator_to_wgs84_is_close():
    geom = _square_wgs84()
    m = reproject_geometry(geom, "EPSG:4326", "EPSG:3857")
    back = reproject_geometry(m, "EPSG:3857", "EPSG:4326")

    assert back.is_empty is False
    assert back.is_valid is True

    # Roundtrip may have small numerical error.
    assert back.equals_exact(geom, tolerance=1e-8) is True
