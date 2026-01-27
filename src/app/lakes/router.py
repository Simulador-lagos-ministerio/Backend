from typing import NoReturn, cast

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from uuid import UUID

from app.postgis_database import get_postgis_db
from app.lakes.models import Lake, LakeDatasetVersion

from app.lakes.schemas import (
    DatasetVersionSummary,
    LakeSummary,
    LakeDetail,
    GridSpec,
    BlockedMaskResponse,
    LayerStats,
    GridManifest,
    GeometryInput,
    GeometryValidationResponse,
    RasterizeResponse,
    GeometryErrorItem,
)

from app.lakes.services import (
    compute_blocked_mask,
    compute_layer_stats,
    validate_and_rasterize_geometry,
    selection_mask_to_bitset_b64,
)

from app.lakes.repository import (
    get_lake as repo_get_lake,
    get_active_dataset_version,
)

from app.lakes.geometry_services import bbox_in_lake_crs, bbox_to_wgs84

router = APIRouter()

_ALLOWED_LAYER_KINDS = {"water", "inhabitants", "ci"}

# Service sometimes returns codes that are not part of GeometryErrorItem Literal
_GEOMETRY_ERROR_CODE_MAP = {
    "GEOMETRY_INVALID": "INVALID_GEOMETRY",
}


def _raise_mapped_error(code: str) -> NoReturn:
    # 404: missing resources (tests expect human messages)
    if code == "LAKE_NOT_FOUND":
        raise HTTPException(status_code=404, detail="Lake not found")
    if code == "DATASET_NOT_FOUND":
        raise HTTPException(status_code=404, detail="Dataset not found")
    if code == "LAYER_NOT_FOUND":
        raise HTTPException(status_code=404, detail="Layer not found")

    # 400: client errors (keep as code strings unless a test requires dict)
    if code in {
        "INVALID_GEOJSON",
        "INVALID_GEOMETRY",
        "EMPTY_SELECTION",
        "INVALID_SELECTION",
        "INVALID_LAYER_KIND",
    }:
        raise HTTPException(status_code=400, detail=code)

    # 500: server/misconfig
    if code in {"DIMENSION_MISMATCH"}:
        raise HTTPException(status_code=500, detail=code)

    # fallback
    raise HTTPException(status_code=400, detail=code)


# ---------------------------
# Existing endpoints
# ---------------------------

@router.get("/lakes", response_model=list[LakeSummary])
def list_lakes(db: Session = Depends(get_postgis_db)):
    lakes = db.query(Lake).all()
    out: list[LakeSummary] = []

    for lake in lakes:
        active = (
            db.query(LakeDatasetVersion)
            .filter(LakeDatasetVersion.lake_id == lake.id)
            .filter(LakeDatasetVersion.status == "ACTIVE")
            .first()
        )

        out.append(
            LakeSummary(
                id=cast(UUID, lake.id),
                name=cast(str, lake.name),
                active_dataset_version_id=cast(UUID, active.id) if active else None,
                grid=GridSpec(
                    rows=cast(int, lake.grid_rows),
                    cols=cast(int, lake.grid_cols),
                    cell_size_m=cast(float, lake.cell_size_m),
                    crs=cast(str, lake.crs),
                    origin_corner=lake.origin_corner,
                    origin_x=cast(float, lake.origin_x),
                    origin_y=cast(float, lake.origin_y),
                ),
            )
        )
    return out


@router.get("/lakes/{lake_id}", response_model=LakeDetail)
def get_lake(lake_id: UUID, db: Session = Depends(get_postgis_db)):
    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise HTTPException(status_code=404, detail="Lake not found")

    active = (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.lake_id == lake.id)
        .filter(LakeDatasetVersion.status == "ACTIVE")
        .first()
    )

    extent_bbox = None

    return LakeDetail(
        id=cast(UUID, lake.id),
        name=cast(str, lake.name),
        active_dataset_version_id=cast(UUID, active.id) if active else None,
        grid=GridSpec(
            rows=cast(int, lake.grid_rows),
            cols=cast(int, lake.grid_cols),
            cell_size_m=cast(float, lake.cell_size_m),
            crs=cast(str, lake.crs),
            origin_corner=lake.origin_corner,
            origin_x=cast(float, lake.origin_x),
            origin_y=cast(float, lake.origin_y),
        ),
        extent_bbox=extent_bbox,
    )


@router.get("/lakes/{lake_id}/blocked-mask", response_model=BlockedMaskResponse)
def get_blocked_mask(lake_id: UUID, db: Session = Depends(get_postgis_db)):
    # Tests expect "Lake not found" when lake_id doesn't exist, even if repo returns DATASET_NOT_FOUND
    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise HTTPException(status_code=404, detail="Lake not found")

    try:
        dv = get_active_dataset_version(db, lake_id)
        return compute_blocked_mask(db, lake_id, cast(UUID, dv.id))
    except ValueError as e:
        _raise_mapped_error(str(e))


@router.get("/lakes/{lake_id}/datasets/active", response_model=DatasetVersionSummary)
def get_active_dataset(lake_id: UUID, db: Session = Depends(get_postgis_db)):
    # Same contract as tests: distinguish lake-not-found vs dataset-not-found
    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise HTTPException(status_code=404, detail="Lake not found")

    try:
        dv = get_active_dataset_version(db, lake_id)
        return DatasetVersionSummary(
            id=cast(UUID, dv.id),
            lake_id=cast(UUID, dv.lake_id),
            version=cast(int, dv.version),
            status=dv.status,
            notes=cast(str, dv.notes),
        )
    except ValueError as e:
        _raise_mapped_error(str(e))


@router.get(
    "/lakes/{lake_id}/datasets/{dataset_version_id}/layers/{layer_kind}/stats",
    response_model=LayerStats,
)
def layer_stats(lake_id: UUID, dataset_version_id: UUID, layer_kind: str, db: Session = Depends(get_postgis_db)):
    # Tests expect 404 (not 400) when layer_kind is not supported
    if layer_kind not in _ALLOWED_LAYER_KINDS:
        raise HTTPException(status_code=404, detail="Layer not found")

    try:
        return compute_layer_stats(db, lake_id, dataset_version_id, layer_kind)
    except ValueError as e:
        _raise_mapped_error(str(e))


# ---------------------------
# New endpoints (Leaflet-Geoman contracts)
# ---------------------------

@router.get("/lakes/{lake_id}/grid", response_model=GridManifest)
def get_lake_grid_manifest(lake_id: UUID, db: Session = Depends(get_postgis_db)):
    try:
        lake = repo_get_lake(db, lake_id)
    except ValueError as e:
        _raise_mapped_error(str(e))

    grid = GridSpec(
        rows=cast(int, lake.grid_rows),
        cols=cast(int, lake.grid_cols),
        cell_size_m=cast(float, lake.cell_size_m),
        crs=cast(str, lake.crs),
        origin_corner=lake.origin_corner,
        origin_x=cast(float, lake.origin_x),
        origin_y=cast(float, lake.origin_y),
    )

    bbox_m = bbox_in_lake_crs(lake)
    bbox_w = bbox_to_wgs84(bbox_m, cast(str, lake.crs))

    return GridManifest(
        lake_id=cast(UUID, lake.id),
        grid=grid,
        bbox_mercator=[bbox_m[0], bbox_m[1], bbox_m[2], bbox_m[3]],
        bbox_wgs84=[bbox_w[0], bbox_w[1], bbox_w[2], bbox_w[3]],
    )


@router.post("/lakes/{lake_id}/validate-geometry", response_model=GeometryValidationResponse)
def validate_geometry(lake_id: UUID, payload: GeometryInput, db: Session = Depends(get_postgis_db)):
    result = validate_and_rasterize_geometry(
        db=db,
        lake_id=lake_id,
        dataset_version_id=payload.dataset_version_id,
        geometry_geojson=payload.geometry,
        geometry_crs=payload.geometry_crs,
        all_touched=payload.all_touched,
    )

    # IMPORTANT: validate-geometry should NOT raise 404; tests expect 200 with minimal payload
    if "code" in result:
        raw_code = result["code"]
        code_norm = _GEOMETRY_ERROR_CODE_MAP.get(raw_code, raw_code)

        lake = result.get("lake")
        dv_id = result.get("dataset_version_id") or payload.dataset_version_id

        # Minimal response when lake is missing (or service couldn't load it)
        if lake is None:
            return GeometryValidationResponse(
                ok=False,
                lake_id=lake_id,
                dataset_version_id=dv_id if dv_id else UUID(int=0),
                rows=0,
                cols=0,
                selected_cells=0,
                blocked_cells=0,
                blocked_breakdown={"water": 0, "inhabitants": 0},
                selection_bitset_base64=None,
                errors=[GeometryErrorItem(code=code_norm, message=result.get("message", ""))],
            )

        rows, cols = int(lake.grid_rows), int(lake.grid_cols)

        selection_b64 = None
        if result.get("selection_mask") is not None:
            selection_b64 = selection_mask_to_bitset_b64(result["selection_mask"])

        selected_cells = 0
        if result.get("selection_mask") is not None:
            selected_cells = int(result["selection_mask"].sum())

        return GeometryValidationResponse(
            ok=False,
            lake_id=lake.id,
            dataset_version_id=cast(UUID, dv_id),
            rows=rows,
            cols=cols,
            selected_cells=selected_cells,
            blocked_cells=0,
            blocked_breakdown={"water": 0, "inhabitants": 0},
            selection_bitset_base64=selection_b64,
            errors=[GeometryErrorItem(code=code_norm, message=result.get("message", ""))],
        )

    lake = result["lake"]
    dv_id = result["dataset_version_id"]
    rows, cols = int(lake.grid_rows), int(lake.grid_cols)

    selection_b64 = selection_mask_to_bitset_b64(result["selection_mask"])

    errors: list[GeometryErrorItem] = []
    if result["blocked_breakdown"]["water"] > 0:
        errors.append(GeometryErrorItem(code="INTERSECTS_WATER", message="Selection intersects water cells"))
    if result["blocked_breakdown"]["inhabitants"] > 0:
        errors.append(GeometryErrorItem(code="INTERSECTS_INHABITANTS", message="Selection intersects inhabited cells"))
    if result["selected_cells"] == 0:
        errors.append(GeometryErrorItem(code="EMPTY_SELECTION", message="0 selected cells"))

    return GeometryValidationResponse(
        ok=bool(result["ok"]),
        lake_id=lake.id,
        dataset_version_id=dv_id,
        rows=rows,
        cols=cols,
        selected_cells=int(result["selected_cells"]),
        blocked_cells=int(result["blocked_cells"]),
        blocked_breakdown=result["blocked_breakdown"],
        selection_bitset_base64=selection_b64,
        errors=errors,
    )


@router.post("/lakes/{lake_id}/rasterize-geometry", response_model=RasterizeResponse)
def rasterize_geometry(lake_id: UUID, payload: GeometryInput, db: Session = Depends(get_postgis_db)):
    result = validate_and_rasterize_geometry(
        db=db,
        lake_id=lake_id,
        dataset_version_id=payload.dataset_version_id,
        geometry_geojson=payload.geometry,
        geometry_crs=payload.geometry_crs,
        all_touched=payload.all_touched,
    )

    # On rasterize we fail hard if invalid:
    # tests expect 400 with {"code": ..., "message": ...} for geometry errors
    if "code" in result:
        code = result["code"]
        msg = result.get("message", "")

        if code == "LAKE_NOT_FOUND":
            raise HTTPException(status_code=404, detail="Lake not found")
        if code == "DATASET_NOT_FOUND":
            raise HTTPException(status_code=404, detail="Dataset not found")

        raise HTTPException(status_code=400, detail={"code": code, "message": msg})

    if not result["ok"]:
        raise HTTPException(status_code=400, detail={"code": "INVALID_SELECTION", "message": "Invalid selection"})

    lake = result["lake"]
    dv_id = result["dataset_version_id"]
    rows, cols = int(lake.grid_rows), int(lake.grid_cols)

    selection_b64 = selection_mask_to_bitset_b64(result["selection_mask"])
    cell_count = int(result["selected_cells"])

    return RasterizeResponse(
        lake_id=lake.id,
        dataset_version_id=dv_id,
        rows=rows,
        cols=cols,
        cell_count=cell_count,
        selection_bitset_base64=selection_b64,
    )
