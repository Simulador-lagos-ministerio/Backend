"""
Geometry utilities for the Lakes domain:
- GeoJSON parsing (Polygon / MultiPolygon)
- CRS reprojection (pyproj)
- Rasterization to grid mask (rasterio.features.rasterize)
- Bitset encoding: packbits (LSB0) + zlib + base64
- Lake bounds helpers (lake CRS bbox and WGS84 bbox)
"""

from __future__ import annotations

import base64
import zlib
from typing import Any, Dict, Tuple

import numpy as np
from pyproj import CRS, Transformer
from rasterio.features import rasterize
from rasterio.transform import from_origin
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shp_transform

from app.lakes.models import Lake
from app.lakes.schemas import GridSpec

# Public encoding contract (keep stable for frontend compatibility).
ENCODING = "bitset+zlib+base64"
BIT_ORDER = "lsb0"
CELL_ORDER = "row_major_cell_id"


class GeometryError(ValueError):
    """Raised for user-provided geometry issues (invalid GeoJSON, invalid geometry)."""


def mask_to_bitset_bytes(mask_bool: np.ndarray) -> bytes:
    """
    Convert a boolean mask to packed bits using row-major order and LSB0 bit order.
    """
    flat = mask_bool.astype(np.uint8).reshape(-1)  # 0/1
    packed = np.packbits(flat, bitorder="little")
    return packed.tobytes()


def bitset_bytes_to_mask(bitset: bytes, rows: int, cols: int) -> np.ndarray:
    """
    Reverse operation of mask_to_bitset_bytes.
    """
    n_bits = rows * cols
    packed = np.frombuffer(bitset, dtype=np.uint8)
    flat = np.unpackbits(packed, bitorder="little")[:n_bits].astype(bool)
    return flat.reshape((rows, cols))


def encode_bitset_zlib_base64(bitset_bytes: bytes, level: int = 6) -> str:
    """
    Compress raw bitset bytes with zlib and encode as base64 ASCII string.
    """
    compressed = zlib.compress(bitset_bytes, level=level)
    return base64.b64encode(compressed).decode("ascii")


def decode_bitset_zlib_base64(b64: str) -> bytes:
    """
    Decode base64 ASCII string and decompress zlib-compressed bytes.
    """
    compressed = base64.b64decode(b64.encode("ascii"))
    return zlib.decompress(compressed)


def mask_to_encoded_bitset(mask_bool: np.ndarray, level: int = 6) -> str:
    """
    Convenience wrapper: boolean mask -> packbits -> zlib -> base64.
    """
    return encode_bitset_zlib_base64(mask_to_bitset_bytes(mask_bool), level=level)


def parse_geojson_geometry(geojson: Dict[str, Any]) -> BaseGeometry:
    """
    Parse a GeoJSON *geometry* dict into a Shapely geometry.

    We accept Polygon and MultiPolygon:
    - Leaflet-Geoman typically emits Polygon
    - MultiPolygon support is a robustness improvement and doesn't harm invariants
      because overlap/blocked checks happen at cell level anyway.
    """
    if not isinstance(geojson, dict) or "type" not in geojson:
        raise GeometryError("Invalid GeoJSON geometry: missing 'type'.")

    geom = shape(geojson)

    if geom.is_empty:
        raise GeometryError("Geometry is empty.")

    if geom.geom_type not in ("Polygon", "MultiPolygon"):
        raise GeometryError(f"Unsupported geometry type: {geom.geom_type}. Use Polygon/MultiPolygon.")

    if not geom.is_valid:
        raise GeometryError("Geometry is not valid (self-intersection or invalid ring).")

    return geom


def reproject_geometry(geom: BaseGeometry, src_crs: str, dst_crs: str) -> BaseGeometry:
    """
    Reproject a Shapely geometry from src_crs to dst_crs.
    CRS strings can be like 'EPSG:4326', 'EPSG:3857', etc.
    """
    src = CRS.from_user_input(src_crs)
    dst = CRS.from_user_input(dst_crs)

    if src == dst:
        return geom

    transformer = Transformer.from_crs(src, dst, always_xy=True)
    return shp_transform(transformer.transform, geom)


def bbox_in_lake_crs(lake: Lake) -> Tuple[float, float, float, float]:
    """
    Compute the lake bbox in its own CRS based on grid specs.
    Assumes origin_corner='top_left'.

    Returns (minx, miny, maxx, maxy) in lake.crs.
    """
    if (lake.origin_corner or "").lower() != "top_left":
        raise ValueError("UNSUPPORTED_ORIGIN_CORNER")

    minx = float(lake.origin_x)
    maxx = float(lake.origin_x + lake.grid_cols * lake.cell_size_m)
    maxy = float(lake.origin_y)
    miny = float(lake.origin_y - lake.grid_rows * lake.cell_size_m)
    return (minx, miny, maxx, maxy)


def bbox_to_wgs84(bbox: Tuple[float, float, float, float], src_crs: str) -> Tuple[float, float, float, float]:
    """
    Transform bbox from src_crs to EPSG:4326 (WGS84).

    Returns (minlon, minlat, maxlon, maxlat).
    """
    minx, miny, maxx, maxy = bbox
    transformer = Transformer.from_crs(CRS.from_user_input(src_crs), CRS.from_epsg(4326), always_xy=True)
    lon1, lat1 = transformer.transform(minx, miny)
    lon2, lat2 = transformer.transform(maxx, maxy)
    return (min(lon1, lon2), min(lat1, lat2), max(lon1, lon2), max(lat1, lat2))


def _grid_transform(grid: GridSpec):
    """
    Build a rasterio affine transform from grid specs. Supports only top-left origin.
    """
    if grid.origin_corner != "top_left":
        raise ValueError("Only origin_corner='top_left' is supported.")
    return from_origin(grid.origin_x, grid.origin_y, grid.cell_size_m, grid.cell_size_m)


def rasterize_geometry_to_mask(
    geom_projected: BaseGeometry,
    grid: GridSpec,
    all_touched: bool = False,
) -> np.ndarray:
    """
    Rasterize a projected geometry into a boolean mask (rows, cols).
    True means the cell is selected.
    """
    transform = _grid_transform(grid)
    out = rasterize(
        [(geom_projected, 1)],
        out_shape=(grid.rows, grid.cols),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=all_touched,
    )
    return out.astype(bool)
