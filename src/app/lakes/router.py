from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from uuid import UUID

from app.postgis_database import get_postgis_db
from app.lakes.models import Lake, LakeDatasetVersion
from app.lakes.schemas import LakeSummary, LakeDetail, GridSpec, BlockedMaskResponse
from app.lakes.services import get_active_dataset_version, compute_blocked_mask

router = APIRouter()

@router.get("/lakes", response_model=list[LakeSummary])
def list_lakes(db: Session = Depends(get_postgis_db)):
    lakes = db.query(Lake).all()
    out = []
    for lake in lakes:
        active = (
            db.query(LakeDatasetVersion)
            .filter(LakeDatasetVersion.lake_id == lake.id)
            .filter(LakeDatasetVersion.status == "ACTIVE")
            .first()
        )
        out.append(
            LakeSummary(
                id=lake.id,
                name=lake.name,
                active_dataset_version_id=active.id if active else None,
                grid=GridSpec(
                    rows=int(lake.grid_rows),
                    cols=int(lake.grid_cols),
                    cell_size_m=float(lake.cell_size_m),
                    crs=lake.crs,
                    origin_corner=lake.origin_corner,
                    origin_x=float(lake.origin_x),
                    origin_y=float(lake.origin_y),
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

    # Extent_bbox opcional: si no cargaste extent_geom, queda None
    extent_bbox = None
    if lake.extent_geom is not None:
        # Evitamos depender de funciones GIS aquí; podés agregar ST_Extent luego.
        # En MVP, lo dejamos como None o se computa en una query posterior.
        extent_bbox = None

    return LakeDetail(
        id=lake.id,
        name=lake.name,
        active_dataset_version_id=active.id if active else None,
        grid=GridSpec(
            rows=int(lake.grid_rows),
            cols=int(lake.grid_cols),
            cell_size_m=float(lake.cell_size_m),
            crs=lake.crs,
            origin_corner=lake.origin_corner,
            origin_x=float(lake.origin_x),
            origin_y=float(lake.origin_y),
        ),
        extent_bbox=extent_bbox,
    )

@router.get("/lakes/{lake_id}/blocked-mask", response_model=BlockedMaskResponse)
def get_blocked_mask(lake_id: UUID, db: Session = Depends(get_postgis_db)):
    try:
        dv = get_active_dataset_version(db, lake_id)
        payload = compute_blocked_mask(db, lake_id, dv.id)
        return payload
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
