import numpy as np
import rasterio
from cachetools import TTLCache
from sqlalchemy.orm import Session
from uuid import UUID

from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer
from app.storage.s3_client import download_to_tempfile

_STATS_CACHE = TTLCache(maxsize=256, ttl=60 * 30)  # 30 min

def _layer_kind_to_db(kind: str) -> str:
    mapping = {
        "water": "WATER",
        "inhabitants": "INHABITANTS",
        "ci": "CI",
    }
    if kind not in mapping:
        raise ValueError("Invalid layer_kind")
    return mapping[kind]

def get_dataset_version(db: Session, dataset_version_id: UUID) -> LakeDatasetVersion:
    dv = db.query(LakeDatasetVersion).filter(LakeDatasetVersion.id == dataset_version_id).first()
    if not dv:
        raise ValueError("Dataset version not found")
    return dv

def get_active_dataset_version(db: Session, lake_id: UUID) -> LakeDatasetVersion:
    dv = (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.lake_id == lake_id)
        .filter(LakeDatasetVersion.status == "ACTIVE")
        .first()
    )
    if not dv:
        raise ValueError("No ACTIVE dataset version for lake")
    return dv

def get_layer(db: Session, dataset_version_id: UUID, layer_kind_api: str) -> LakeLayer:
    kind_db = _layer_kind_to_db(layer_kind_api)
    layer = (
        db.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dataset_version_id)
        .filter(LakeLayer.layer_kind == kind_db)
        .first()
    )
    if not layer:
        raise ValueError(f"Missing layer {layer_kind_api}")
    return layer

def compute_layer_stats(db: Session, lake_id: UUID, dataset_version_id: UUID, layer_kind_api: str) -> dict:
    cache_key = (str(lake_id), str(dataset_version_id), layer_kind_api)
    if cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key]

    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise ValueError("Lake not found")

    layer = get_layer(db, dataset_version_id, layer_kind_api)

    # Download and read raster
    local_path = download_to_tempfile(layer.storage_uri)
    with rasterio.open(local_path) as src:
        arr = src.read(1)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if arr.shape != (rows, cols):
        raise ValueError("Layer dimensions do not match lake grid")

    # Mask nodata if available; else use full array
    nodata = layer.nodata
    if nodata is not None:
        data = arr[arr != float(nodata)]
    else:
        data = arr.reshape(-1)

    # Handle all-nodata / empty
    if data.size == 0:
        stats = {"count": 0}
    else:
        # For water we care about counts of 0/1; for others numeric stats
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
        "dataset_version_id": dataset_version_id,
        "layer_kind": layer_kind_api,
        "rows": rows,
        "cols": cols,
        "dtype": layer.dtype,
        "nodata": float(nodata) if nodata is not None else None,
        "stats": stats,
    }
    _STATS_CACHE[cache_key] = payload
    return payload
