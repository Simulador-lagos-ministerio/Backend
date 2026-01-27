import numpy as np
import pytest
from shapely.geometry import Polygon

from app.lakes.schemas import GridSpec
from app.lakes.geometry_services import grid_transform, rasterize_geometry_to_mask


def test_grid_transform_top_left_ok():
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=10.0,
    )
    t = grid_transform(grid)
    assert t is not None


def test_grid_transform_rejects_non_top_left():
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",  # cambiamos luego
        origin_x=0.0,
        origin_y=10.0,
    )
    # Forzamos un valor inválido (pydantic literal suele impedirlo, así que usamos model_copy)
    grid2 = grid.model_copy(update={"origin_corner": "bottom_left"})  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        grid_transform(grid2)


def test_rasterize_returns_bool_mask_with_correct_shape():
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=10.0,
    )

    geom = Polygon([(0.0, 10.0), (0.0, 9.0), (1.0, 9.0), (1.0, 10.0), (0.0, 10.0)])
    mask = rasterize_geometry_to_mask(geom, grid, all_touched=False)

    assert mask.shape == (10, 10)
    assert mask.dtype == bool


def test_rasterize_aligned_block_expected_cells():
    """
    Grid: origin (0,10), cell=1, rows/cols=10.
    Polygon covers x:[2,4] y:[6,8] in projected coordinates.

    En un grid top-left:
    - columnas: x 0..10
    - filas: y 10..0

    Esperamos seleccionar exactamente 2x2 celdas:
    cols 2-3 y rows correspondientes a y 8..6.
    """
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=10.0,
    )

    geom = Polygon([
        (2.0, 8.0),
        (2.0, 6.0),
        (4.0, 6.0),
        (4.0, 8.0),
        (2.0, 8.0),
    ])

    mask = rasterize_geometry_to_mask(geom, grid, all_touched=False)

    assert int(mask.sum()) == 4

    # Validamos que el bloque esperado esté en True
    # y=8..6 corresponde a filas: row = (origin_y - y)/cell
    # y in [6,8] -> rows 2..4, pero como bordes, el bloque interior son rows 2,3 y cols 2,3
    expected = np.zeros((10, 10), dtype=bool)
    expected[2:4, 2:4] = True

    assert np.array_equal(mask, expected)


def test_all_touched_selects_more_or_equal_cells_on_border_touch():
    grid = GridSpec(
        rows=10,
        cols=10,
        cell_size_m=1.0,
        crs="EPSG:3857",
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=10.0,
    )

    # Polígono muy fino que pasa cerca de bordes (diagonal)
    geom = Polygon([
        (0.1, 9.9),
        (0.1, 9.1),
        (9.9, 0.1),
        (9.9, 0.9),
        (0.1, 9.9),
    ])

    mask_false = rasterize_geometry_to_mask(geom, grid, all_touched=False)
    mask_true = rasterize_geometry_to_mask(geom, grid, all_touched=True)

    assert int(mask_true.sum()) >= int(mask_false.sum())
