from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any
from uuid import UUID

OriginCorner = Literal["top_left"]
LayerKind = Literal["water", "inhabitants", "ci"]

class GridSpec(BaseModel):
    rows: int = Field(..., ge=1)
    cols: int = Field(..., ge=1)
    cell_size_m: float = Field(..., gt=0)
    crs: str
    origin_corner: OriginCorner = "top_left"
    origin_x: float
    origin_y: float

class LakeSummary(BaseModel):
    id: UUID
    name: str
    active_dataset_version_id: Optional[UUID] = None
    grid: GridSpec

class LakeDetail(BaseModel):
    id: UUID
    name: str
    active_dataset_version_id: Optional[UUID] = None
    grid: GridSpec
    extent_bbox: Optional[Dict[str, float]] = None

class BlockedMaskResponse(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    rows: int
    cols: int
    encoding: Literal["bitset+zlib+base64"]
    bit_order: Literal["lsb0"] = "lsb0"
    cell_order: Literal["row_major_cell_id"] = "row_major_cell_id"
    blocked_bitset_base64: str
    blocked_count: Optional[int] = None
    water_count: Optional[int] = None
    inhabited_count: Optional[int] = None

class DatasetVersionSummary(BaseModel):
    id: UUID
    lake_id: UUID
    version: int
    status: Literal["DRAFT", "ACTIVE", "DEPRECATED"]
    notes: Optional[str] = None

class LayerStats(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    layer_kind: LayerKind
    rows: int
    cols: int
    dtype: str
    nodata: Optional[float] = None
    stats: Dict[str, Any]