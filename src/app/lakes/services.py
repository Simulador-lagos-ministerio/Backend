from __future__ import annotations

from typing import Optional
from uuid import UUID

import numpy as np
from cachetools import TTLCache
from sqlalchemy.orm import Session

from app.lakes.geometry_services import (
    BIT_ORDER,
    CELL_ORDER,
    ENCODING,
    GeometryError,
    bbox_in_lake_crs,
    bbox_to_wgs84,
    mask_to_encoded_bitset,
    parse_geojson_geometry,
    rasterize_geometry_to_mask,
    reproject_geometry,
)
from app.lakes.repository import (
    get_active_dataset_version,
    get_lake,
    get_layer,
    list_lakes as repo_list_lakes,
    read_layer_array,
    resolve_dataset_version,
)
from app.lakes.schemas import (
    BlockedMaskResponse,
    DatasetVersionSummary,
    GeometryErrorItem,
    GeometryValidationResponse,
    GridManifest,
    GridSpec,
    LakeDetail,
    LakeSummary,
    LayerStats,
)


_STATS_CACHE = TTLCache(maxsize=256, ttl=60 * 30)      # 30 minutes
_BLOCKED_CACHE = TTLCache(maxsize=128, ttl=60 * 10)    # 10 minutes


def _grid_spec_from_lake(lake) -> GridSpec:
    """
    Build GridSpec from a Lake ORM object.
    """
    return GridSpec(
        rows=int(lake.grid_rows),
        cols=int(lake.grid_cols),
        cell_size_m=float(lake.cell_size_m),
        crs=str(lake.crs),
        origin_corner=str(lake.origin_corner),
        origin_x=float(lake.origin_x),
        origin_y=float(lake.origin_y),
    )


def _grid_bounds_payload(lake) -> tuple[list[float], list[float], list[list[float]]]:
    """
    Returns:
      - bbox_lake_crs: [minx, miny, maxx, maxy] in lake CRS
      - bbox_wgs84:    [minlon, minlat, maxlon, maxlat] in EPSG:4326
      - leaflet_bounds: [[minLat, minLon], [maxLat, maxLon]]
    """
    minx, miny, maxx, maxy = bbox_in_lake_crs(lake)
    bbox_lake_crs = [minx, miny, maxx, maxy]

    minlon, minlat, maxlon, maxlat = bbox_to_wgs84((minx, miny, maxx, maxy), str(lake.crs))
    bbox_wgs84 = [minlon, minlat, maxlon, maxlat]

    leaflet_bounds = [[minlat, minlon], [maxlat, maxlon]]
    return bbox_lake_crs, bbox_wgs84, leaflet_bounds


# -----------------------------
# Lake discovery endpoints
# -----------------------------

def list_lakes(db: Session) -> list[LakeSummary]:
    lakes = repo_list_lakes(db)
    out: list[LakeSummary] = []

    for lake in lakes:
        dv = get_active_dataset_version(db, lake.id)
        out.append(
            LakeSummary(
                id=lake.id,
                name=str(lake.name),
                active_dataset_version_id=(dv.id if dv else None),
                grid=_grid_spec_from_lake(lake),
            )
        )
    return out


def get_lake_detail(db: Session, lake_id: UUID) -> LakeDetail:
    lake = get_lake(db, lake_id)
    dv = get_active_dataset_version(db, lake.id)

    bbox_lake_crs, bbox_wgs84, leaflet_bounds = _grid_bounds_payload(lake)

    return LakeDetail(
        id=lake.id,
        name=str(lake.name),
        active_dataset_version_id=(dv.id if dv else None),
        grid=_grid_spec_from_lake(lake),
        bbox_lake_crs=bbox_lake_crs,
        bbox_wgs84=bbox_wgs84,
        leaflet_bounds=leaflet_bounds,
    )


def get_grid_manifest(db: Session, lake_id: UUID) -> GridManifest:
    lake = get_lake(db, lake_id)
    bbox_lake_crs, bbox_wgs84, leaflet_bounds = _grid_bounds_payload(lake)

    return GridManifest(
        lake_id=lake.id,
        grid=_grid_spec_from_lake(lake),
        bbox_lake_crs=bbox_lake_crs,
        bbox_wgs84=bbox_wgs84,
        leaflet_bounds=leaflet_bounds,
    )


def get_active_dataset(db: Session, lake_id: UUID) -> DatasetVersionSummary:
    lake = get_lake(db, lake_id)
    dv = get_active_dataset_version(db, lake.id)
    if not dv:
        raise ValueError("DATASET_NOT_FOUND")

    # NOTE: assumes dv.version and dv.notes exist. If not, adjust here.
    return DatasetVersionSummary(
        id=dv.id,
        lake_id=dv.lake_id,
        version=int(getattr(dv, "version", 1)),
        status=str(dv.status),
        notes=getattr(dv, "notes", None),
        meta=getattr(dv, "meta", None),
    )


# -----------------------------
# Raster / Layer endpoints
# -----------------------------

def compute_blocked_mask(db: Session, lake_id: UUID, dataset_version_id: Optional[UUID]) -> BlockedMaskResponse:
    """
    Return blocked mask (water OR inhabitants) encoded as bitset+zlib+base64.
    """
    cache_key = (str(lake_id), str(dataset_version_id) if dataset_version_id else "ACTIVE")
    if cache_key in _BLOCKED_CACHE:
        return _BLOCKED_CACHE[cache_key]

    lake = get_lake(db, lake_id)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)

    water_layer = get_layer(db, dv.id, "water")
    inhab_layer = get_layer(db, dv.id, "inhabitants")

    water = read_layer_array(water_layer)
    inhab = read_layer_array(inhab_layer)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if water.shape != (rows, cols) or inhab.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    # NOTE: For safety, treat nodata as blocked (if present).
    water_nodata = getattr(water_layer, "nodata", None)
    inhab_nodata = getattr(inhab_layer, "nodata", None)

    water_valid = np.ones((rows, cols), dtype=bool) if water_nodata is None else (water != float(water_nodata))
    inhab_valid = np.ones((rows, cols), dtype=bool) if inhab_nodata is None else (inhab != float(inhab_nodata))

    water_mask = (water != 0) & water_valid
    inhab_mask = (inhab > 0) & inhab_valid

    nodata_mask = (~water_valid) | (~inhab_valid)
    blocked = water_mask | inhab_mask | nodata_mask

    bitset_b64 = mask_to_encoded_bitset(blocked, level=6)

    payload = BlockedMaskResponse(
        lake_id=lake.id,
        dataset_version_id=dv.id,
        rows=rows,
        cols=cols,
        encoding=ENCODING,
        bit_order=BIT_ORDER,
        cell_order=CELL_ORDER,
        blocked_bitset_base64=bitset_b64,
        blocked_count=int(blocked.sum()),
        water_count=int(water_mask.sum()),
        inhabited_count=int(inhab_mask.sum()),
    )

    _BLOCKED_CACHE[cache_key] = payload
    return payload


def compute_layer_stats(db: Session, lake_id: UUID, dataset_version_id: UUID, layer_kind: str) -> LayerStats:
    """
    Compute basic statistics for a layer.
    """
    cache_key = (str(lake_id), str(dataset_version_id), layer_kind)
    if cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key]

    lake = get_lake(db, lake_id)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)
    layer = get_layer(db, dv.id, layer_kind)

    arr = read_layer_array(layer)
    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if arr.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    nodata = getattr(layer, "nodata", None)
    valid = np.ones(arr.shape, dtype=bool) if nodata is None else (arr != float(nodata))
    values = arr[valid].reshape(-1)

    if values.size == 0:
        stats = {"count": 0}
    else:
        if layer_kind == "water":
            # Fix: exclude nodata from water counting
            water_mask = (arr != 0) & valid
            water_count = int(water_mask.sum())
            stats = {
                "count": int(values.size),
                "water_count": water_count,
                "water_fraction": float(water_count / (rows * cols)),
            }
        elif layer_kind == "inhabitants":
            inhabited_cells = int(((arr > 0) & valid).sum())
            total_pop = float(np.sum(np.clip(arr[valid], 0, None)))
            stats = {
                "count": int(values.size),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "p50": float(np.percentile(values, 50)),
                "p95": float(np.percentile(values, 95)),
                "inhabited_cells": inhabited_cells,
                "inhabited_fraction": float(inhabited_cells / (rows * cols)),
                "total_inhabitants": total_pop,
            }
        else:  # ci
            stats = {
                "count": int(values.size),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "p50": float(np.percentile(values, 50)),
                "p95": float(np.percentile(values, 95)),
            }

    payload = LayerStats(
        lake_id=lake.id,
        dataset_version_id=dv.id,
        layer_kind=layer_kind,  # type: ignore[arg-type]
        rows=rows,
        cols=cols,
        dtype=str(getattr(layer, "dtype", "unknown")),
        nodata=float(nodata) if nodata is not None else None,
        stats=stats,
    )

    _STATS_CACHE[cache_key] = payload
    return payload


# -----------------------------
# Geometry validation (UX-friendly)
# -----------------------------

def validate_and_rasterize_geometry(
    db: Session,
    lake_id: UUID,
    geometry_obj: dict,
    geometry_crs: str,
    all_touched: bool,
    dataset_version_id: Optional[UUID],
) -> GeometryValidationResponse:
    """
    UX-friendly validation:
      - returns ok=false for user selection issues (invalid geojson, empty selection, intersects water/inhabitants, etc.)
      - raises ValueError for hard failures (lake/dataset/layers missing, dimension mismatch)
    """
    lake = get_lake(db, lake_id)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)

    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    grid = _grid_spec_from_lake(lake)

    errors: list[GeometryErrorItem] = []
    selected_cells = 0
    blocked_cells = 0
    breakdown: dict[str, int] = {"water": 0, "inhabitants": 0, "nodata": 0}
    selection_bitset_b64: Optional[str] = None

    # 1) Parse GeoJSON -> shapely geometry
    try:
        geom = parse_geojson_geometry(geometry_obj)
    except GeometryError as e:
        errors.append(GeometryErrorItem(code="INVALID_GEOJSON", message=str(e)))
        return GeometryValidationResponse(
            ok=False,
            lake_id=lake.id,
            dataset_version_id=dv.id,
            rows=rows,
            cols=cols,
            selected_cells=0,
            blocked_cells=0,
            blocked_breakdown=breakdown,
            selection_bitset_base64=None,
            errors=errors,
        )

    # 2) Reproject to lake CRS
    try:
        geom_proj = reproject_geometry(geom, geometry_crs, str(lake.crs))
    except Exception as e:
        errors.append(GeometryErrorItem(code="INVALID_GEOMETRY", message=f"Reprojection error: {e}"))
        return GeometryValidationResponse(
            ok=False,
            lake_id=lake.id,
            dataset_version_id=dv.id,
            rows=rows,
            cols=cols,
            selected_cells=0,
            blocked_cells=0,
            blocked_breakdown=breakdown,
            selection_bitset_base64=None,
            errors=errors,
        )

    # 3) Rasterize -> selection mask
    sel_mask = rasterize_geometry_to_mask(geom_proj, grid, all_touched=all_touched)
    selected_cells = int(sel_mask.sum())

    if selected_cells == 0:
        errors.append(GeometryErrorItem(code="EMPTY_SELECTION", message="0 selected cells (geometry outside grid)."))
        return GeometryValidationResponse(
            ok=False,
            lake_id=lake.id,
            dataset_version_id=dv.id,
            rows=rows,
            cols=cols,
            selected_cells=0,
            blocked_cells=0,
            blocked_breakdown=breakdown,
            selection_bitset_base64=None,
            errors=errors,
        )

    # Encode selection even for invalid selection (useful for debug / preview)
    selection_bitset_b64 = mask_to_encoded_bitset(sel_mask, level=9)

    # 4) Load constraint layers
    water_layer = get_layer(db, dv.id, "water")
    inhab_layer = get_layer(db, dv.id, "inhabitants")

    water = read_layer_array(water_layer)
    inhab = read_layer_array(inhab_layer)

    if water.shape != (rows, cols) or inhab.shape != (rows, cols):
        raise ValueError("DIMENSION_MISMATCH")

    water_nodata = getattr(water_layer, "nodata", None)
    inhab_nodata = getattr(inhab_layer, "nodata", None)

    water_valid = np.ones((rows, cols), dtype=bool) if water_nodata is None else (water != float(water_nodata))
    inhab_valid = np.ones((rows, cols), dtype=bool) if inhab_nodata is None else (inhab != float(inhab_nodata))
    nodata_mask = (~water_valid) | (~inhab_valid)

    water_hits = int(((water != 0) & water_valid & sel_mask).sum())
    inhab_hits = int(((inhab > 0) & inhab_valid & sel_mask).sum())
    nodata_hits = int((nodata_mask & sel_mask).sum())

    breakdown["water"] = water_hits
    breakdown["inhabitants"] = inhab_hits
    breakdown["nodata"] = nodata_hits

    blocked_cells = int((sel_mask & ((water != 0) | (inhab > 0) | nodata_mask)).sum())

    # 5) Build UX errors
    if nodata_hits > 0:
        errors.append(GeometryErrorItem(code="INTERSECTS_NODATA", message="Selection intersects nodata cells."))
    if water_hits > 0:
        errors.append(GeometryErrorItem(code="INTERSECTS_WATER", message="Selection intersects water cells."))
    if inhab_hits > 0:
        errors.append(GeometryErrorItem(code="INTERSECTS_INHABITANTS", message="Selection intersects inhabited cells."))

    ok = blocked_cells == 0

    return GeometryValidationResponse(
        ok=ok,
        lake_id=lake.id,
        dataset_version_id=dv.id,
        rows=rows,
        cols=cols,
        selected_cells=selected_cells,
        blocked_cells=blocked_cells,
        blocked_breakdown=breakdown,
        selection_bitset_base64=selection_bitset_b64,
        errors=errors,
    )
