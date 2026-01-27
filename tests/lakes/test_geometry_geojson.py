"""Unit tests for GeoJSON parsing."""
import pytest

from app.lakes.geometry_services import GeometryError, parse_geojson_geometry


def test_parse_geojson_polygon_ok():
    geojson = {
        "type": "Polygon",
        "coordinates": [[
            [0.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]],
    }
    geom = parse_geojson_geometry(geojson)
    assert geom.geom_type == "Polygon"
    assert geom.is_valid is True
    assert geom.is_empty is False


def test_parse_geojson_multipolygon_ok():
    geojson = {
        "type": "MultiPolygon",
        "coordinates": [
            [[
                [0.0, 0.0],
                [0.0, 1.0],
                [1.0, 1.0],
                [1.0, 0.0],
                [0.0, 0.0],
            ]],
            [[
                [2.0, 2.0],
                [2.0, 3.0],
                [3.0, 3.0],
                [3.0, 2.0],
                [2.0, 2.0],
            ]],
        ],
    }
    geom = parse_geojson_geometry(geojson)
    assert geom.geom_type == "MultiPolygon"
    assert geom.is_valid is True
    assert geom.is_empty is False


@pytest.mark.parametrize("bad_input", [None, 123, "x", [], {}, {"foo": "bar"}])
def test_parse_geojson_missing_type_rejected(bad_input):
    with pytest.raises(GeometryError):
        parse_geojson_geometry(bad_input)  # type: ignore[arg-type]


def test_parse_geojson_unsupported_type_rejected():
    geojson = {"type": "Point", "coordinates": [0.0, 0.0]}
    with pytest.raises(GeometryError) as e:
        parse_geojson_geometry(geojson)
    assert "Unsupported geometry type" in str(e.value)


def test_parse_geojson_empty_geometry_rejected():
    # Empty GeometryCollection.
    geojson = {"type": "GeometryCollection", "geometries": []}
    with pytest.raises(GeometryError) as e:
        parse_geojson_geometry(geojson)
    assert "Geometry is empty" in str(e.value) or "Unsupported geometry type" in str(e.value)


def test_parse_geojson_invalid_self_intersection_rejected():
    # "Bow-tie" polygon (self-intersecting).
    geojson = {
        "type": "Polygon",
        "coordinates": [[
            [0.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]],
    }
    with pytest.raises(GeometryError) as e:
        parse_geojson_geometry(geojson)
    assert "not valid" in str(e.value).lower()
