from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

# -----------------------------
# Common domain literals
# -----------------------------

OriginCorner = Literal["top_left"]
LayerKind = Literal["water", "inhabitants", "ci"]

ENCODING_LITERAL = Literal["bitset+zlib+base64"]
BIT_ORDER_LITERAL = Literal["lsb0"]
CELL_ORDER_LITERAL = Literal["row_major_cell_id"]


# -----------------------------
# Lake / Grid schemas
# -----------------------------

class GridSpec(BaseModel):
    """
    Canonical grid spec used across the lakes domain.
    """
    rows: int = Field(..., ge=1)
    cols: int = Field(..., ge=1)
    cell_size_m: float = Field(..., gt=0)
    crs: str
    origin_corner: OriginCorner = "top_left"
    origin_x: float
    origin_y: float


class LakeSummary(BaseModel):
    """
    Light payload for listing lakes (dropdown / selection UI).
    """
    id: UUID
    name: str
    active_dataset_version_id: Optional[UUID] = None
    grid: GridSpec


class LakeDetail(BaseModel):
    """
    Detailed payload for a single lake.
    Includes grid and bounds needed by the frontend.
    """
    id: UUID
    name: str
    active_dataset_version_id: Optional[UUID] = None
    grid: GridSpec

    # Bounds in lake CRS (same CRS as grid.crs) -> [minx, miny, maxx, maxy]
    bbox_lake_crs: List[float]

    # Bounds in WGS84 -> [minlon, minlat, maxlon, maxlat]
    bbox_wgs84: List[float]

    # Leaflet-friendly bounds -> [[southWestLat, southWestLng], [northEastLat, northEastLng]]
    leaflet_bounds: List[List[float]]


class GridManifest(BaseModel):
    """
    Minimal “map bootstrap” payload for Leaflet:
    - grid spec
    - bbox in lake CRS
    - bbox in WGS84
    - Leaflet bounds for fitBounds()
    """
    lake_id: UUID
    grid: GridSpec
    bbox_lake_crs: List[float]   # [minx, miny, maxx, maxy] in lake CRS
    bbox_wgs84: List[float]      # [minlon, minlat, maxlon, maxlat] in EPSG:4326
    leaflet_bounds: List[List[float]]  # [[minLat, minLon], [maxLat, maxLon]]


# -----------------------------
# Dataset / Layer schemas
# -----------------------------

class DatasetVersionSummary(BaseModel):
    id: UUID
    lake_id: UUID
    version: int
    status: Literal["ACTIVE", "INACTIVE"]
    notes: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class LayerStats(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    layer_kind: LayerKind
    rows: int
    cols: int
    dtype: str
    nodata: Optional[float] = None
    stats: Dict[str, Any]


class BlockedMaskResponse(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    rows: int
    cols: int

    encoding: ENCODING_LITERAL
    bit_order: BIT_ORDER_LITERAL = "lsb0"
    cell_order: CELL_ORDER_LITERAL = "row_major_cell_id"

    blocked_bitset_base64: str

    blocked_count: Optional[int] = None
    water_count: Optional[int] = None
    inhabited_count: Optional[int] = None


# -----------------------------
# Geometry / Drawing schemas
# -----------------------------

class GeometryInput(BaseModel):
    """
    Leaflet-Geoman typically emits EPSG:4326 coordinates.
    We accept GeoJSON *geometry* (not Feature) to keep contracts simple.
    """
    dataset_version_id: Optional[UUID] = None  # if None -> ACTIVE
    geometry: Dict[str, Any]
    geometry_crs: str = "EPSG:4326"
    all_touched: bool = False


GeometryErrorCode = Literal[
    "EMPTY_SELECTION",
    "INVALID_GEOJSON",
    "UNSUPPORTED_GEOMETRY",
    "INVALID_GEOMETRY",
    "INTERSECTS_WATER",
    "INTERSECTS_INHABITANTS",
    "INTERSECTS_NODATA",
    "DIMENSION_MISMATCH",
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

    # Breakdown counts are useful for UX messaging.
    blocked_breakdown: Dict[str, int]  # e.g., {"water": n, "inhabitants": n, "nodata": n}

    encoding: ENCODING_LITERAL = "bitset+zlib+base64"
    bit_order: BIT_ORDER_LITERAL = "lsb0"
    cell_order: CELL_ORDER_LITERAL = "row_major_cell_id"

    # Useful for preview/persistence even when ok=false (if selection is non-empty).
    selection_bitset_base64: Optional[str] = None

    errors: List[GeometryErrorItem] = Field(default_factory=list)


class RasterizeResponse(BaseModel):
    lake_id: UUID
    dataset_version_id: UUID
    rows: int
    cols: int

    encoding: ENCODING_LITERAL = "bitset+zlib+base64"
    bit_order: BIT_ORDER_LITERAL = "lsb0"
    cell_order: CELL_ORDER_LITERAL = "row_major_cell_id"

    cell_count: int
    selection_bitset_base64: str
