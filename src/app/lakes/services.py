from __future__ import annotations

from uuid import UUID
from typing import Optional, Dict
from cachetools import TTLCache
from sqlalchemy.orm import Session
import numpy as np
import rasterio

from app.lakes.repository import get_lake, resolve_dataset_version, get_layer, read_layer_array
from app.lakes.geometry_services import (
    ENCODING, BIT_ORDER, CELL_ORDER,
    mask_to_encoded_bitset, parse_geojson_geometry, GeometryError, reproject_geometry, rasterize_geometry_to_mask
)
from app.lakes.schemas import GridSpec as GridSpecSchema
from app.storage.s3_client import download_to_tempfile

_STATS_CACHE = TTLCache(maxsize=256, ttl=60 * 30)  # 30 min

_BLOCKED_CACHE = TTLCache(maxsize=128, ttl=60 * 10)  # 10 min


def compute_blocked_mask(db: Session, lake_id: UUID, dataset_version_id: UUID) -> dict:
    cache_key = (str(lake_id), str(dataset_version_id))
    if cache_key in _BLOCKED_CACHE:
        return _BLOCKED_CACHE[cache_key]

    lake = get_lake(db, lake_id)

    # Validate dataset_version belongs to lake (or raise)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)

    water_layer = get_layer(db, dv.id, "water")
    inhab_layer = get_layer(db, dv.id, "inhabitants")

    water = read_layer_array(water_layer)
    inhab = read_layer_array(inhab_layer)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if water.shape != (rows, cols) or inhab.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    water_bool = water != 0
    inhab_bool = inhab > 0
    blocked = water_bool | inhab_bool

    b64 = mask_to_encoded_bitset(blocked, level=6)

    result = {
        "lake_id": lake_id,
        "dataset_version_id": dv.id,
        "rows": rows,
        "cols": cols,
        "encoding": ENCODING,
        "bit_order": BIT_ORDER,
        "cell_order": CELL_ORDER,
        "blocked_bitset_base64": b64,
        "blocked_count": int(blocked.sum()),
        "water_count": int(water_bool.sum()),
        "inhabited_count": int(inhab_bool.sum()),
    }

    _BLOCKED_CACHE[cache_key] = result
    return result

def compute_layer_stats(db: Session, lake_id: UUID, dataset_version_id: UUID, layer_kind_api: str) -> dict:
    cache_key = (str(lake_id), str(dataset_version_id), layer_kind_api)
    if cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key]

    lake = get_lake(db, lake_id)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)
    layer = get_layer(db, dv.id, layer_kind_api)

    local_path = download_to_tempfile(layer.storage_uri)
    with rasterio.open(local_path) as src:
        arr = src.read(1)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if arr.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    nodata = layer.nodata
    data = arr[arr != float(nodata)] if nodata is not None else arr.reshape(-1)

    if data.size == 0:
        stats = {"count": 0}
    else:
        if layer_kind_api == "water":
            water_count = int((arr != 0).sum())
            stats = {
                "count": int(data.size),
                "water_count": water_count,
                "water_fraction": float(water_count / (rows * cols)),
            }
        elif layer_kind_api == "inhabitants":
            inhabited = int((arr > 0).sum())
            total_pop = float(np.sum(np.clip(arr, 0, None)))
            stats = {
                "count": int(data.size),
                "min": float(np.min(data)),
                "max": float(np.max(data)),
                "p50": float(np.percentile(data, 50)),
                "p95": float(np.percentile(data, 95)),
                "inhabited_cells": inhabited,
                "inhabited_fraction": float(inhabited / (rows * cols)),
                "total_inhabitants": total_pop,
            }
        else:  # ci
            stats = {
                "count": int(data.size),
                "min": float(np.min(data)),
                "max": float(np.max(data)),
                "p50": float(np.percentile(data, 50)),
                "p95": float(np.percentile(data, 95)),
            }

    payload = {
        "lake_id": lake_id,
        "dataset_version_id": dv.id,
        "layer_kind": layer_kind_api,
        "rows": rows,
        "cols": cols,
        "dtype": layer.dtype,
        "nodata": float(nodata) if nodata is not None else None,
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
) -> Dict:
    """
    Returns a normalized dict for geometry validation + selection mask.
    No duplicated access logic: uses lakes.repository.
    """
    try:
        lake = get_lake(db, lake_id)
        dv = resolve_dataset_version(db, lake_id, dataset_version_id)
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

    # Parse GeoJSON -> Shapely
    try:
        geom = parse_geojson_geometry(geometry_geojson)
    except GeometryError as e:
        return {
            "ok": False,
            "code": "INVALID_GEOJSON",
            "message": str(e),
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": None,
        }
    except Exception as e:
        return {
            "ok": False,
            "code": "INVALID_GEOJSON",
            "message": f"GeoJSON parse error: {e}",
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": None,
        }

    # Reproject to lake CRS
    try:
        geom_proj = reproject_geometry(geom, geometry_crs, lake.crs)
    except Exception as e:
        return {
            "ok": False,
            "code": "INVALID_GEOMETRY",
            "message": f"Reprojection error: {e}",
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": None,
        }

    grid = GridSpecSchema(
        rows=rows,
        cols=cols,
        cell_size_m=float(lake.cell_size_m),
        crs=lake.crs,
        origin_corner=lake.origin_corner,
        origin_x=float(lake.origin_x),
        origin_y=float(lake.origin_y),
    )

    mask = rasterize_geometry_to_mask(geom_proj, grid, all_touched=all_touched)
    selected_cells = int(mask.sum())

    if selected_cells == 0:
        return {
            "ok": False,
            "code": "EMPTY_SELECTION",
            "message": "Geometry does not intersect the lake grid (0 selected cells).",
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": mask,
        }

    # Load constraint layers
    try:
        water_layer = get_layer(db, dv.id, "water")
        inh_layer = get_layer(db, dv.id, "inhabitants")
    except ValueError:
        return {
            "ok": False,
            "code": "LAYER_NOT_FOUND",
            "message": "Missing required layer(s): water/inhabitants",
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": mask,
        }

    water = read_layer_array(water_layer)
    inh = read_layer_array(inh_layer)

    if water.shape != (rows, cols) or inh.shape != (rows, cols):
        return {
            "ok": False,
            "code": "DIMENSION_MISMATCH",
            "message": "Layer dimensions do not match lake grid.",
            "lake": lake,
            "dataset_version_id": dv.id,
            "selection_mask": mask,
        }

    water_hits = int((water[mask] != 0).sum())
    inh_hits = int((inh[mask] > 0).sum())

    blocked_union = ((water != 0) | (inh > 0))
    blocked_cells = int(blocked_union[mask].sum())

    return {
        "ok": blocked_cells == 0,
        "lake": lake,
        "dataset_version_id": dv.id,
        "selection_mask": mask,
        "selected_cells": selected_cells,
        "blocked_cells": blocked_cells,
        "blocked_breakdown": {"water": water_hits, "inhabitants": inh_hits},
    }


def selection_mask_to_bitset_b64(mask: np.ndarray) -> str:
    return mask_to_encoded_bitset(mask, level=9)