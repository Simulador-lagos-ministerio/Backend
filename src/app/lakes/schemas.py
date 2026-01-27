from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, List, Any
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

# --- Geometry / Drawing contracts (Leaflet-Geoman) ---

class GridManifest(BaseModel):
    lake_id: UUID
    grid: GridSpec
    bbox_mercator: List[float]  # [minx, miny, maxx, maxy] in lake CRS (usually EPSG:3857)
    bbox_wgs84: List[float]     # [minlon, minlat, maxlon, maxlat] in EPSG:4326


class GeometryInput(BaseModel):
    # Leaflet-Geoman will send GeoJSON geometry (Polygon/MultiPolygon)
    dataset_version_id: Optional[UUID] = None  # if None -> ACTIVE version
    geometry: Dict[str, Any]
    geometry_crs: str = "EPSG:4326"
    all_touched: bool = False  # rasterize option


GeometryErrorCode = Literal[
    "EMPTY_SELECTION",
    "INVALID_GEOJSON",
    "UNSUPPORTED_GEOMETRY",
    "INVALID_GEOMETRY",
    "INTERSECTS_WATER",
    "INTERSECTS_INHABITANTS",
    "DIMENSION_MISMATCH",
    "LAKE_NOT_FOUND",
    "DATASET_NOT_FOUND",
    "LAYER_NOT_FOUND",
]

class GeometryErrorItem(BaseModel):
    code: GeometryErrorCode
    message: str


class GeometryValidationResponse(BaseModel):
    ok: bool
    lake_id: UUID
    dataset_version_id: UUID

    rows: int
    cols: int

    selected_cells: int
    blocked_cells: int
    blocked_breakdown: Dict[str, int]  # {"water": n, "inhabitants": n}

    encoding: Literal["bitset+zlib+base64"] = "bitset+zlib+base64"
    bit_order: Literal["lsb0"] = "lsb0"
    cell_order: Literal["row_major_cell_id"] = "row_major_cell_id"

    # Useful for preview / persistence
    selection_bitset_base64: Optional[str] = None

    errors: List[GeometryErrorItem] = Field(default_factory=list)


class RasterizeResponse(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    rows: int
    cols: int

    encoding: Literal["bitset+zlib+base64"] = "bitset+zlib+base64"
    bit_order: Literal["lsb0"] = "lsb0"
    cell_order: Literal["row_major_cell_id"] = "row_major_cell_id"

    cell_count: int
    selection_bitset_base64: str