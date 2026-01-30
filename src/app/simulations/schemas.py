from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class SimulationCreate(BaseModel):
    lake_id: UUID
    name: str = Field(min_length=1, max_length=200)
    dataset_version_id: Optional[UUID] = None  # default: use active dataset


class SubdivisionCreate(BaseModel):
    geometry: Dict[str, Any]
    geometry_crs: str = Field(default="EPSG:4326")
    all_touched: bool = Field(default=False)

    inhabitants: int = Field(ge=0)
    impact_factor: float = Field(ge=0.0, le=1.0)

    # Optional but must match simulation.dataset_version_id if provided
    dataset_version_id: Optional[UUID] = None


class SubdivisionOut(BaseModel):
    id: UUID
    simulation_id: UUID

    geometry: Dict[str, Any]
    geometry_crs: str
    all_touched: bool

    selected_cells: int
    selection_bitset_base64: str

    inhabitants: int
    impact_factor: float


class SimulationOut(BaseModel):
    id: UUID
    user_id: UUID
    lake_id: UUID
    dataset_version_id: UUID
    name: str
    subdivision_count: int
    total_selected_cells: int
    occupied_bitset_base64: Optional[str] = None


class SimulationDetail(SimulationOut):
    subdivisions: List[SubdivisionOut]


class RunOut(BaseModel):
    id: UUID
    simulation_id: UUID
    status: str
    created_at: str
    inputs_snapshot: Dict[str, Any]
    results_total: Optional[Dict[str, Any]] = None
    results_by_subdivision: Optional[Dict[str, Any]] = None
