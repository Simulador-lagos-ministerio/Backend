from __future__ import annotations

from typing import Optional
from uuid import UUID

import numpy as np
import rasterio
from sqlalchemy.orm import Session

from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer
from app.storage.s3_client import download_to_tempfile, remove_tempfile
from app.common.errors import not_found, bad_request

# Map API layer kinds -> DB enum strings
_LAYER_KIND_MAP = {
    "water": "WATER",
    "inhabitants": "INHABITANTS",
    "ci": "CI",
}


def get_lake(db: Session, lake_id: UUID) -> Lake:
    """
    Fetch a lake by id or raise a stable error code.
    """
    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise not_found("LAKE_NOT_FOUND", "Lake not found.", {"lake_id": str(lake_id)})
    return lake


def list_lakes(db: Session) -> list[Lake]:
    """
    Return all lakes. In production it's valid to return an empty list.
    We only enforce the origin_corner constraint if lakes exist.
    """
    lakes = db.query(Lake).order_by(Lake.name.asc()).all()
    
    return lakes


def get_active_dataset_version(db: Session, lake_id: UUID) -> Optional[LakeDatasetVersion]:
    """
    Return ACTIVE dataset version for a lake, or None if it doesn't exist.
    """
    return (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.lake_id == lake_id)
        .filter(LakeDatasetVersion.status == "ACTIVE")
        .first()
    )


def resolve_dataset_version(db: Session, lake_id: UUID, dataset_version_id: Optional[UUID]) -> LakeDatasetVersion:
    """
    Resolve a dataset version. If dataset_version_id is None -> ACTIVE.
    """
    if dataset_version_id is None:
        dv = get_active_dataset_version(db, lake_id)
        if not dv:
            raise not_found("DATASET_NOT_FOUND", "Active dataset version not found for lake.", {"lake_id": str(lake_id)})
        return dv

    dv = (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.id == dataset_version_id)
        .filter(LakeDatasetVersion.lake_id == lake_id)
        .first()
    )
    if not dv:
        raise not_found("DATASET_NOT_FOUND", "Dataset version not found for lake.", {"lake_id": str(lake_id), "dataset_version_id": str(dataset_version_id)})
    return dv


def get_layer(db: Session, dataset_version_id: UUID, layer_kind_api: str) -> LakeLayer:
    """
    layer_kind_api: "water" | "inhabitants" | "ci"
    """
    if layer_kind_api not in _LAYER_KIND_MAP:
        raise bad_request("LAYER_INVALID_KIND", "Invalid layer kind.", {"layer_kind": layer_kind_api})

    kind_db = _LAYER_KIND_MAP[layer_kind_api]
    layer = (
        db.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dataset_version_id)
        .filter(LakeLayer.layer_kind == kind_db)
        .first()
    )
    if not layer:
        raise not_found("LAYER_NOT_FOUND", "Layer not found for dataset version.", {"dataset_version_id": str(dataset_version_id), "layer_kind": layer_kind_api})
    return layer


def read_layer_array(layer: LakeLayer) -> np.ndarray:
    """
    Download the layer COG from storage_uri into a temp file and read band 1.
    """
    local_path = download_to_tempfile(str(layer.storage_uri))
    try:
        with rasterio.open(local_path) as src:
            return src.read(1)
    finally:
        remove_tempfile(local_path)
