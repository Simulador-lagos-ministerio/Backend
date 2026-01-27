import pytest
from shapely.geometry import Polygon

from app.lakes.geometry_services import reproject_geometry


def _square_wgs84():
    # Cuadrado 0.01° x 0.01° cerca del origen
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

    # Las coords deberían cambiar (grados -> metros)
    assert out.equals_exact(geom, tolerance=0.0) is False

    # Cerca del origen: (0,0) se transforma cerca de (0,0) en mercator
    x0, y0 = list(out.exterior.coords)[0]
    assert abs(x0) < 1e-6
    assert abs(y0) < 1e-6


def test_reproject_roundtrip_wgs84_to_mercator_to_wgs84_is_close():
    geom = _square_wgs84()
    m = reproject_geometry(geom, "EPSG:4326", "EPSG:3857")
    back = reproject_geometry(m, "EPSG:3857", "EPSG:4326")

    assert back.is_empty is False
    assert back.is_valid is True

    # roundtrip puede tener error numérico pequeño
    assert back.equals_exact(geom, tolerance=1e-8) is True
