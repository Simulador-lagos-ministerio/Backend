"""Lake service layer: stats, caching, and geometry validation helpers."""
from __future__ import annotations

from typing import Any, Optional
from uuid import UUID

import numpy as np
from cachetools import TTLCache
from sqlalchemy.orm import Session

from app.lakes.geometry_services import (
    BIT_ORDER,
    CELL_ORDER,
    ENCODING,
    GeometryError,
    mask_to_encoded_bitset,
    parse_geojson_geometry,
    rasterize_geometry_to_mask,
    reproject_geometry,
)
from app.lakes.repository import get_lake, get_layer, read_layer_array, resolve_dataset_version
from app.lakes.schemas import GridSpec as GridSpecSchema
# Imported to keep a stable monkeypatch target in tests.
from app.storage.s3_client import download_to_tempfile  # noqa: F401

# Short-lived caches to avoid re-reading rasters on hot endpoints.
_STATS_CACHE = TTLCache(maxsize=256, ttl=60 * 30)  # 30 minutes
_BLOCKED_CACHE = TTLCache(maxsize=128, ttl=60 * 10)  # 10 minutes


def compute_blocked_mask(db: Session, lake_id: UUID, dataset_version_id: UUID) -> dict[str, Any]:
    """Return the blocked mask (water OR inhabitants) as a bitset payload."""
    cache_key = (str(lake_id), str(dataset_version_id))
    if cache_key in _BLOCKED_CACHE:
        return _BLOCKED_CACHE[cache_key]

    lake = get_lake(db, lake_id)

    # Validate dataset_version belongs to the lake (or raise).
    dataset_version = resolve_dataset_version(db, lake_id, dataset_version_id)

    water_layer = get_layer(db, dataset_version.id, "water")
    inhabitants_layer = get_layer(db, dataset_version.id, "inhabitants")

    water_array = read_layer_array(water_layer)
    inhabitants_array = read_layer_array(inhabitants_layer)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if water_array.shape != (rows, cols) or inhabitants_array.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    water_mask = water_array != 0
    inhabitants_mask = inhabitants_array > 0
    blocked_mask = water_mask | inhabitants_mask

    bitset_b64 = mask_to_encoded_bitset(blocked_mask, level=6)

    result = {
        "lake_id": lake_id,
        "dataset_version_id": dataset_version.id,
        "rows": rows,
        "cols": cols,
        "encoding": ENCODING,
        "bit_order": BIT_ORDER,
        "cell_order": CELL_ORDER,
        "blocked_bitset_base64": bitset_b64,
        "blocked_count": int(blocked_mask.sum()),
        "water_count": int(water_mask.sum()),
        "inhabited_count": int(inhabitants_mask.sum()),
    }

    _BLOCKED_CACHE[cache_key] = result
    return result


def compute_layer_stats(
    db: Session,
    lake_id: UUID,
    dataset_version_id: UUID,
    layer_kind_api: str,
) -> dict[str, Any]:
    """Compute per-layer stats payload and cache it briefly."""
    cache_key = (str(lake_id), str(dataset_version_id), layer_kind_api)
    if cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key]

    lake = get_lake(db, lake_id)
    dataset_version = resolve_dataset_version(db, lake_id, dataset_version_id)
    layer = get_layer(db, dataset_version.id, layer_kind_api)

    layer_array = read_layer_array(layer)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if layer_array.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    nodata_value = layer.nodata
    if nodata_value is None:
        valid_mask = np.ones(layer_array.shape, dtype=bool)
    else:
        valid_mask = layer_array != float(nodata_value)

    # Slice only valid (non-nodata) values for stats.
    valid_values = layer_array[valid_mask].reshape(-1)

    if valid_values.size == 0:
        stats = {"count": 0}
    else:
        if layer_kind_api == "water":
            water_mask = (layer_array != 0) & valid_mask
            water_count = int(water_mask.sum())
            stats = {
                "count": int(valid_values.size),
                "water_count": water_count,
                "water_fraction": float(water_count / (rows * cols)),
            }
        elif layer_kind_api == "inhabitants":
            inhabited_cells = int((layer_array > 0).sum())
            total_pop = float(np.sum(np.clip(layer_array, 0, None)))
            stats = {
                "count": int(valid_values.size),
                "min": float(np.min(valid_values)),
                "max": float(np.max(valid_values)),
                "p50": float(np.percentile(valid_values, 50)),
                "p95": float(np.percentile(valid_values, 95)),
                "inhabited_cells": inhabited_cells,
                "inhabited_fraction": float(inhabited_cells / (rows * cols)),
                "total_inhabitants": total_pop,
            }
        else:  # ci
            stats = {
                "count": int(valid_values.size),
                "min": float(np.min(valid_values)),
                "max": float(np.max(valid_values)),
                "p50": float(np.percentile(valid_values, 50)),
                "p95": float(np.percentile(valid_values, 95)),
            }

    payload = {
        "lake_id": lake_id,
        "dataset_version_id": dataset_version.id,
        "layer_kind": layer_kind_api,
        "rows": rows,
        "cols": cols,
        "dtype": layer.dtype,
        "nodata": float(nodata_value) if nodata_value is not None else None,
        "stats": stats,
    }
    _STATS_CACHE[cache_key] = payload
    return payload


def validate_and_rasterize_geometry(
    db: Session,
    lake_id: UUID,
    dataset_version_id: Optional[UUID],
    geometry_geojson: dict,
    geometry_crs: str,
    all_touched: bool = False,
) -> dict[str, Any]:
    """
    Return a normalized dict for geometry validation + selection mask.
    All data access is delegated to lakes.repository helpers.
    """
    try:
        lake = get_lake(db, lake_id)
        dataset_version = resolve_dataset_version(db, lake_id, dataset_version_id)
    except ValueError as e:
        code = str(e)
        return {
            "ok": False,
            "code": code,
            "message": code,
            "lake": None,
            "dataset_version_id": dataset_version_id,
            "selection_mask": None,
        }

    rows = int(lake.grid_rows)
    cols = int(lake.grid_cols)

    # Parse GeoJSON into a Shapely geometry.
    try:
        geometry = parse_geojson_geometry(geometry_geojson)
    except GeometryError as e:
        return {
            "ok": False,
            "code": "INVALID_GEOJSON",
            "message": str(e),
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": None,
        }
    except Exception as e:
        return {
            "ok": False,
            "code": "INVALID_GEOJSON",
            "message": f"GeoJSON parse error: {e}",
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": None,
        }

    # Reproject to the lake CRS.
    try:
        projected_geometry = reproject_geometry(geometry, geometry_crs, str(lake.crs))
    except Exception as e:
        return {
            "ok": False,
            "code": "INVALID_GEOMETRY",
            "message": f"Reprojection error: {e}",
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": None,
        }

    grid = GridSpecSchema(
        rows=rows,
        cols=cols,
        cell_size_m=float(lake.cell_size_m),
        crs=str(lake.crs),
        origin_corner=lake.origin_corner,
        origin_x=float(lake.origin_x),
        origin_y=float(lake.origin_y),
    )

    selection_mask = rasterize_geometry_to_mask(projected_geometry, grid, all_touched=all_touched)
    selected_cells = int(selection_mask.sum())

    if selected_cells == 0:
        return {
            "ok": False,
            "code": "EMPTY_SELECTION",
            "message": "Geometry does not intersect the lake grid (0 selected cells).",
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": selection_mask,
        }

    # Load constraint layers used to validate the selection.
    try:
        water_layer = get_layer(db, dataset_version.id, "water")
        inhabitants_layer = get_layer(db, dataset_version.id, "inhabitants")
    except ValueError:
        return {
            "ok": False,
            "code": "LAYER_NOT_FOUND",
            "message": "Missing required layer(s): water/inhabitants",
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": selection_mask,
        }

    water_array = read_layer_array(water_layer)
    inhabitants_array = read_layer_array(inhabitants_layer)

    if water_array.shape != (rows, cols) or inhabitants_array.shape != (rows, cols):
        return {
            "ok": False,
            "code": "DIMENSION_MISMATCH",
            "message": "Layer dimensions do not match lake grid.",
            "lake": lake,
            "dataset_version_id": dataset_version.id,
            "selection_mask": selection_mask,
        }

    water_hits = int((water_array[selection_mask] != 0).sum())
    inhabitants_hits = int((inhabitants_array[selection_mask] > 0).sum())

    blocked_union = (water_array != 0) | (inhabitants_array > 0)
    blocked_cells = int(blocked_union[selection_mask].sum())

    return {
        "ok": blocked_cells == 0,
        "lake": lake,
        "dataset_version_id": dataset_version.id,
        "selection_mask": selection_mask,
        "selected_cells": selected_cells,
        "blocked_cells": blocked_cells,
        "blocked_breakdown": {"water": water_hits, "inhabitants": inhabitants_hits},
    }


def selection_mask_to_bitset_b64(mask: np.ndarray) -> str:
    """Encode a boolean selection mask to the bitset+zlib+base64 format."""
    return mask_to_encoded_bitset(mask, level=9)
