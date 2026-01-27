from __future__ import annotations

from typing import Optional
from uuid import UUID

import numpy as np
import rasterio
from sqlalchemy.orm import Session

from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer
from app.storage.s3_client import download_to_tempfile


_LAYER_KIND_MAP = {
    "water": "WATER",
    "inhabitants": "INHABITANTS",
    "ci": "CI",
}


def get_lake(db: Session, lake_id: UUID) -> Lake:
    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise ValueError("LAKE_NOT_FOUND")
    return lake


def get_active_dataset_version(db: Session, lake_id: UUID) -> LakeDatasetVersion:
    dv = (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.lake_id == lake_id)
        .filter(LakeDatasetVersion.status == "ACTIVE")
        .first()
    )
    if not dv:
        raise ValueError("DATASET_NOT_FOUND")
    return dv


def resolve_dataset_version(db: Session, lake_id: UUID, dataset_version_id: Optional[UUID]) -> LakeDatasetVersion:
    if dataset_version_id is None:
        return get_active_dataset_version(db, lake_id)

    dv = (
        db.query(LakeDatasetVersion)
        .filter(LakeDatasetVersion.id == dataset_version_id)
        .filter(LakeDatasetVersion.lake_id == lake_id)
        .first()
    )
    if not dv:
        raise ValueError("DATASET_NOT_FOUND")
    return dv


def get_layer(db: Session, dataset_version_id: UUID, layer_kind_api: str) -> LakeLayer:
    """
    layer_kind_api: "water" | "inhabitants" | "ci"
    """
    if layer_kind_api not in _LAYER_KIND_MAP:
        raise ValueError("LAYER_NOT_FOUND")

    kind_db = _LAYER_KIND_MAP[layer_kind_api]
    layer = (
        db.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dataset_version_id)
        .filter(LakeLayer.layer_kind == kind_db)
        .first()
    )
    if not layer:
        raise ValueError("LAYER_NOT_FOUND")
    return layer


def read_layer_array(layer: LakeLayer) -> np.ndarray:
    """
    Downloads layer COG from storage_uri to a temp file and reads band 1.
    """
    local_path = download_to_tempfile(str(layer.storage_uri))
    with rasterio.open(local_path) as src:
        return src.read(1)
