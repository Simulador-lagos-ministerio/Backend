# tests/unit/lakes/test_geometry_services_rasterize.py
"""
Unit tests for rasterization against a lake grid.

We enforce:
- rasterize returns correct shape
- empty selection is detected (either via error or zero-count)
- origin_corner must be top_left
"""

from __future__ import annotations

import numpy as np
import pytest
from types import SimpleNamespace

import app.lakes.geometry_services as gs  # type: ignore
from app.lakes.schemas import GridSpec


def test_rasterize_geometry_to_mask_shape():
    assert hasattr(gs, "rasterize_geometry_to_mask"), "rasterize_geometry_to_mask must exist"
    assert hasattr(gs, "parse_geojson_geometry"), "parse_geojson_geometry must exist"

    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
    )

    geom = {
        "type": "Polygon",
        "coordinates": [[[0.1, -0.9], [0.9, -0.9], [0.9, -0.1], [0.1, -0.1], [0.1, -0.9]]],
    }
    g = gs.parse_geojson_geometry(geom)
    mask = gs.rasterize_geometry_to_mask(g, grid, all_touched=False)
    assert isinstance(mask, np.ndarray)
    assert mask.shape == (10, 10)
    assert mask.sum() >= 1


def test_rasterize_empty_selection_outside_bbox():
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
    )
    geom = {
        "type": "Polygon",
        "coordinates": [[[100, 100], [101, 100], [101, 101], [100, 101], [100, 100]]],
    }
    g = gs.parse_geojson_geometry(geom)
    mask = gs.rasterize_geometry_to_mask(g, grid, all_touched=False)
    assert mask.sum() == 0


def test_rasterize_rejects_non_top_left_origin():
    grid = SimpleNamespace(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="bottom_left",  # invalid by design
        origin_x=0.0,
        origin_y=0.0,
    )
    geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
    g = gs.parse_geojson_geometry(geom)
    with pytest.raises(Exception):
        gs.rasterize_geometry_to_mask(g, grid, all_touched=False)
