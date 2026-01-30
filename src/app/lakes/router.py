from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.postgis_database import get_db
from app.common.responses import json_ok, json_fail

from app.lakes.schemas import GeometryInput
from app.lakes.services import (
    compute_blocked_mask,
    compute_layer_stats,
    get_active_dataset,
    get_grid_manifest,
    get_lake_detail,
    list_lakes,
    validate_and_rasterize_geometry,
)

router = APIRouter(prefix="/lakes")


def _map_lakes_error(code: str) -> tuple[int, str]:
    """
    Convert domain error codes into HTTP status and a human-readable message.
    Keep this mapping stable for production clients.
    """
    if code in {"LAKE_NOT_FOUND", "DATASET_NOT_FOUND", "LAYER_NOT_FOUND"}:
        return 404, code
    if code in {"UNSUPPORTED_ORIGIN_CORNER", "SOME_LAKES_UNSUPPORTED_ORIGIN_CORNER"}:
        return 400, code
    if code in {"DIMENSION_MISMATCH"}:
        return 500, code
    return 400, code


@router.get("")
def get_lakes(db: Session = Depends(get_db)):
    """
    List lakes (for lake selection UI).
    """
    try:
        items = list_lakes(db)
        return json_ok(data=[i.model_dump() for i in items])
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(code=str(e), message=msg, status_code=status)


@router.get("/{lake_id}")
def get_lake(lake_id: UUID, db: Session = Depends(get_db)):
    """
    Lake detail including grid and bounds.
    """
    try:
        detail = get_lake_detail(db, lake_id)
        return json_ok(data=detail.model_dump())
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(code=str(e), message=msg, status_code=status, meta={"lake_id": str(lake_id)})


@router.get("/{lake_id}/grid")
def lake_grid(lake_id: UUID, db: Session = Depends(get_db)):
    """
    Map bootstrap endpoint for Leaflet:
    returns grid spec + bounds in CRS and WGS84.
    """
    try:
        manifest = get_grid_manifest(db, lake_id)
        return json_ok(data=manifest.model_dump())
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(code=str(e), message=msg, status_code=status, meta={"lake_id": str(lake_id)})


@router.get("/{lake_id}/datasets/active")
def active_dataset(lake_id: UUID, db: Session = Depends(get_db)):
    """
    Return the ACTIVE dataset version for this lake.
    """
    try:
        dv = get_active_dataset(db, lake_id)
        return json_ok(data=dv.model_dump())
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(code=str(e), message=msg, status_code=status, meta={"lake_id": str(lake_id)})


@router.post("/{lake_id}/validate-geometry")
def validate_geometry(lake_id: UUID, payload: GeometryInput, db: Session = Depends(get_db)):
    """
    UX-friendly:
    - always returns HTTP 200 with ok=true/false for drawing/selection issues
    - returns 4xx/5xx for hard failures (missing lake/dataset/layers, misconfig)
    """
    try:
        res = validate_and_rasterize_geometry(
            db,
            lake_id=lake_id,
            geometry_obj=payload.geometry,
            geometry_crs=payload.geometry_crs,
            all_touched=payload.all_touched,
            dataset_version_id=payload.dataset_version_id,
        )
    except ValueError as e:
        # Hard failures -> 4xx/5xx
        status, msg = _map_lakes_error(str(e))
        return json_fail(code=str(e), message=msg, status_code=status, meta={"lake_id": str(lake_id)})

    if not res.ok:
        return json_fail(
            code="INVALID_SELECTION",
            message="Geometry selection is invalid.",
            meta={
                "selected_cells": res.selected_cells,
                "water_hits": res.blocked_breakdown.get("water", 0),
                "inhabitants_hits": res.blocked_breakdown.get("inhabitants", 0),
                "nodata_hits": res.blocked_breakdown.get("nodata", 0),
                "blocked_cells": res.blocked_cells,
            },
            data=res.model_dump(),
            status_code=200,
        )

    return json_ok(data=res.model_dump())


@router.get("/{lake_id}/datasets/{dataset_version_id}/layers/{layer_kind}/stats")
def layer_stats(lake_id: UUID, dataset_version_id: UUID, layer_kind: str, db: Session = Depends(get_db)):
    """
    Layer statistics endpoint.
    """
    try:
        stats = compute_layer_stats(db, lake_id=lake_id, dataset_version_id=dataset_version_id, layer_kind=layer_kind)
        return json_ok(data=stats.model_dump())
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(
            code=str(e),
            message=msg,
            status_code=status,
            meta={"lake_id": str(lake_id), "dataset_version_id": str(dataset_version_id), "layer_kind": layer_kind},
        )


@router.get("/{lake_id}/blocked-mask")
def blocked_mask(lake_id: UUID, dataset_version_id: Optional[UUID] = None, db: Session = Depends(get_db)):
    """
    Return blocked mask for water OR inhabitants (and nodata as blocked).
    dataset_version_id is optional; if omitted, uses ACTIVE dataset.
    """
    try:
        payload = compute_blocked_mask(db, lake_id=lake_id, dataset_version_id=dataset_version_id)
        return json_ok(data=payload.model_dump())
    except ValueError as e:
        status, msg = _map_lakes_error(str(e))
        return json_fail(
            code=str(e),
            message=msg,
            status_code=status,
            meta={"lake_id": str(lake_id), "dataset_version_id": str(dataset_version_id) if dataset_version_id else "ACTIVE"},
        )
