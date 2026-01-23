import base64
import zlib
from uuid import UUID

import numpy as np
import rasterio
from cachetools import TTLCache
from sqlalchemy.orm import Session

from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer
from app.storage.s3_client import download_to_tempfile

# RAM cache: Crucial for (lake_id, dataset_version_id) -> BlockedMaskResponse dict
_BLOCKED_CACHE = TTLCache(maxsize=128, ttl=60 * 10)  # 10 min

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

def get_layer(db: Session, dataset_version_id: UUID, kind: str) -> LakeLayer:
    # kind must be: 'WATER' / 'INHABITANTS' / 'CI'
    layer = (
        db.query(LakeLayer)
        .filter(LakeLayer.dataset_version_id == dataset_version_id)
        .filter(LakeLayer.layer_kind == kind)
        .first()
    )
    if not layer:
        raise ValueError(f"Missing layer {kind}")
    return layer

def _read_band_as_array(local_path: str) -> np.ndarray:
    with rasterio.open(local_path) as src:
        arr = src.read(1)  # band 1
        return arr

def _pack_blocked_bitset(blocked_bool: np.ndarray) -> bytes:
    # Row-major flatten -> packbits little-endian
    flat = blocked_bool.astype(np.uint8).reshape(-1)  # 0/1
    packed = np.packbits(flat, bitorder="little")
    return packed.tobytes()

def compute_blocked_mask(db: Session, lake_id: UUID, dataset_version_id: UUID) -> dict:
    cache_key = (str(lake_id), str(dataset_version_id))
    if cache_key in _BLOCKED_CACHE:
        return _BLOCKED_CACHE[cache_key]

    lake = db.query(Lake).filter(Lake.id == lake_id).first()
    if not lake:
        raise ValueError("Lake not found")

    water_layer = get_layer(db, dataset_version_id, "WATER")
    inhab_layer = get_layer(db, dataset_version_id, "INHABITANTS")

    # Download COGs to temporary files (simple and robust).
    water_path = download_to_tempfile(water_layer.storage_uri)
    inhab_path = download_to_tempfile(inhab_layer.storage_uri)

    water = _read_band_as_array(water_path)
    inhab = _read_band_as_array(inhab_path)

    # Dimension validation
    rows, cols = int(lake.grid_rows), int(lake.grid_cols)
    if water.shape != (rows, cols) or inhab.shape != (rows, cols):
        raise ValueError("Layer dimensions do not match lake grid")

    water_bool = water != 0
    inhab_bool = inhab > 0
    blocked = water_bool | inhab_bool

    packed_bytes = _pack_blocked_bitset(blocked)
    compressed = zlib.compress(packed_bytes, level=6)
    b64 = base64.b64encode(compressed).decode("ascii")

    result = {
        "lake_id": lake_id,
        "dataset_version_id": dataset_version_id,
        "rows": rows,
        "cols": cols,
        "encoding": "bitset+zlib+base64",
        "bit_order": "lsb0",
        "cell_order": "row_major_cell_id",
        "blocked_bitset_base64": b64,
        "blocked_count": int(blocked.sum()),
        "water_count": int(water_bool.sum()),
        "inhabited_count": int(inhab_bool.sum()),
    }
    _BLOCKED_CACHE[cache_key] = result
    return result
