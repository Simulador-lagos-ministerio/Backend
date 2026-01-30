# tests/_helpers.py
"""
Shared helper utilities for tests.

Includes:
- Bitset decoding (zlib + base64) with LSB0 + row-major convention
- GeoJSON polygon builders aligned to "top_left" lake grid origin
- Raster cell search helpers
"""

from __future__ import annotations

import base64
import zlib
from pathlib import Path
from typing import Callable, Tuple

import numpy as np


def tests_dir() -> Path:
    return Path(__file__).parent


def fixtures_dir() -> Path:
    return tests_dir() / "fixtures"


def rasters_dir() -> Path:
    return fixtures_dir() / "rasters"


def decode_zlib_base64_to_bytes(b64: str) -> bytes:
    compressed = base64.b64decode(b64.encode("ascii"))
    return zlib.decompress(compressed)


def unpack_lsb0_row_major(bitset_bytes: bytes, rows: int, cols: int) -> np.ndarray:
    """
    Convert packed bytes -> boolean mask using LSB0 (bitorder='little') and row-major ordering.
    """
    n_bits = rows * cols
    packed = np.frombuffer(bitset_bytes, dtype=np.uint8)
    flat = np.unpackbits(packed, bitorder="little")[:n_bits].astype(bool)
    return flat.reshape((rows, cols))


def decode_mask_from_bitset_b64(bitset_b64: str, rows: int, cols: int) -> np.ndarray:
    raw = decode_zlib_base64_to_bytes(bitset_b64)
    return unpack_lsb0_row_major(raw, rows, cols)


def polygon_geojson_for_cell(
    origin_x: float,
    origin_y: float,
    cell_size: float,
    row: int,
    col: int,
    inset: float = 1e-6,
) -> dict:
    """
    Build a GeoJSON Polygon that targets exactly one grid cell (row, col)
    assuming origin_corner="top_left".

    Coordinates are in lake CRS units (typically meters if EPSG:3857).
    """
    minx = origin_x + col * cell_size + inset
    maxx = origin_x + (col + 1) * cell_size - inset
    maxy = origin_y - row * cell_size - inset
    miny = origin_y - (row + 1) * cell_size + inset

    return {
        "type": "Polygon",
        "coordinates": [[
            [minx, miny],
            [maxx, miny],
            [maxx, maxy],
            [minx, maxy],
            [minx, miny],
        ]],
    }


def find_first_cell(arr: np.ndarray, predicate: Callable[[float], bool]) -> Tuple[int, int]:
    """
    Return the first (row, col) matching predicate, else raise.
    """
    rows, cols = arr.shape
    for r in range(rows):
        for c in range(cols):
            if predicate(float(arr[r, c])):
                return r, c
    raise AssertionError("No raster cell matched the predicate.")
