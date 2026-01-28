"""Pydantic schemas for simulations API."""
from __future__ import annotations

from typing import Literal, Optional, Any, Dict, List
from uuid import UUID

from pydantic import BaseModel, Field


SimulationStatus = Literal["DRAFT", "FINALIZED"]


class SimulationCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    dataset_version_id: UUID


class SimulationSummary(BaseModel):
    id: UUID
    lake_id: UUID
    dataset_version_id: UUID
    name: str
    status: SimulationStatus
    subdivision_count: int
    total_selected_cells: int


class SubdivisionCreate(BaseModel):
    dataset_version_id: Optional[UUID] = None  # optional: if not provided, use simulation.dataset_version_id
    geometry_crs: str
    all_touched: bool = False
    geometry: Dict[str, Any]  # GeoJSON Feature or Geometry (we handle in services)


class SubdivisionOut(BaseModel):
    id: UUID
    simulation_id: UUID
    selected_cells: int
    selection_bitset_base64: str


class SimulationDetail(SimulationSummary):
    subdivisions: List[SubdivisionOut] = []
