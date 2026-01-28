"""FastAPI routes for simulations and subdivisions."""
from __future__ import annotations

from typing import NoReturn, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.postgis_database import get_postgis_db
from app.users.services import get_current_user
from app.users.models import User

from app.simulations.schemas import (
    SimulationCreate,
    SimulationSummary,
    SimulationDetail,
    SubdivisionCreate,
    SubdivisionOut,
)
from app.simulations.services import (
    create_simulation,
    add_subdivision,
    delete_subdivision,
    finalize_simulation,
)
from app.simulations.repository import get_simulation, list_subdivisions, list_simulations_by_user_and_lake


router = APIRouter()


def _raise_sim_error(code: str) -> NoReturn:
    """Map domain errors to HTTP responses with stable contracts."""
    if code in {"SIMULATION_NOT_FOUND", "LAKE_NOT_FOUND", "DATASET_NOT_FOUND", "SUBDIVISION_NOT_FOUND"}:
        raise HTTPException(status_code=404, detail=code)

    if code in {"FORBIDDEN"}:
        raise HTTPException(status_code=403, detail=code)

    if code in {"SIMULATION_FINALIZED", "MAX_SUBDIVISIONS_EXCEEDED"}:
        raise HTTPException(status_code=409, detail=code)

    if code in {
        "INVALID_GEOJSON",
        "INVALID_GEOMETRY",
        "UNSUPPORTED_GEOMETRY",
        "EMPTY_SELECTION",
        "INVALID_SELECTION",
        "SUBDIVISION_OVERLAP",
        "DATASET_VERSION_IMMUTABLE",
    }:
        raise HTTPException(status_code=400, detail=code)

    raise HTTPException(status_code=400, detail=code)


@router.post("/lakes/{lake_id}/simulations", response_model=SimulationSummary)
def create_sim(lake_id: UUID, payload: SimulationCreate, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    try:
        sim = create_simulation(
            db,
            user_id=cast(int, user.id),
            lake_id=lake_id,
            dataset_version_id=payload.dataset_version_id,
            name=payload.name,
        )
        return SimulationSummary(
            id=cast(UUID, sim.id),
            lake_id=cast(UUID, sim.lake_id),
            dataset_version_id=cast(UUID, sim.dataset_version_id),
            name=cast(str, sim.name),
            status=sim.status,  # type: ignore
            subdivision_count=cast(int, sim.subdivision_count),
            total_selected_cells=cast(int, sim.total_selected_cells),
        )
    except ValueError as e:
        _raise_sim_error(str(e))


@router.get("/simulations/{simulation_id}", response_model=SimulationDetail)
def get_sim(simulation_id: UUID, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    try:
        sim = get_simulation(db, simulation_id)
        if cast(bool, sim.user_id != cast(int, user.id)):
            _raise_sim_error("FORBIDDEN")

        subs = list_subdivisions(db, cast(UUID, sim.id))
        return SimulationDetail(
            id=cast(UUID, sim.id),
            lake_id=cast(UUID, sim.lake_id),
            dataset_version_id=cast(UUID, sim.dataset_version_id),
            name=cast(str, sim.name),
            status=sim.status,  # type: ignore
            subdivision_count=cast(int, sim.subdivision_count),
            total_selected_cells=cast(int, sim.total_selected_cells),
            subdivisions=[
                SubdivisionOut(
                    id=cast(UUID, s.id),
                    simulation_id=cast(UUID, s.simulation_id),
                    selected_cells=cast(int, s.selected_cells),
                    selection_bitset_base64=cast(str, s.selection_bitset_base64),
                )
                for s in subs
            ],
        )
    except ValueError as e:
        _raise_sim_error(str(e))


@router.post("/simulations/{simulation_id}/subdivisions", response_model=SubdivisionOut)
def add_sub(simulation_id: UUID, payload: SubdivisionCreate, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    try:
        sub = add_subdivision(
            db,
            simulation_id=simulation_id,
            user_id=cast(int, user.id),
            geometry_geojson=payload.geometry,
            geometry_crs=payload.geometry_crs,
            all_touched=payload.all_touched,
            dataset_version_id=payload.dataset_version_id,
        )
        return SubdivisionOut(
            id=cast(UUID, sub.id),
            simulation_id=cast(UUID, sub.simulation_id),
            selected_cells=cast(int, sub.selected_cells),
            selection_bitset_base64=cast(str, sub.selection_bitset_base64),
        )
    except ValueError as e:
        _raise_sim_error(str(e))


@router.post("/simulations/{simulation_id}/finalize", response_model=SimulationSummary)
def finalize(simulation_id: UUID, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    try:
        sim = finalize_simulation(db, simulation_id=simulation_id, user_id=cast(int, user.id))
        return SimulationSummary(
            id=cast(UUID, sim.id),
            lake_id=cast(UUID, sim.lake_id),
            dataset_version_id=cast(UUID, sim.dataset_version_id),
            name=cast(str, sim.name),
            status=sim.status,  # type: ignore
            subdivision_count=cast(int, sim.subdivision_count),
            total_selected_cells=cast(int, sim.total_selected_cells),
        )
    except ValueError as e:
        _raise_sim_error(str(e))


@router.delete("/simulations/{simulation_id}/subdivisions/{subdivision_id}", response_model=SimulationSummary)
def remove_sub(simulation_id: UUID, subdivision_id: UUID, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    try:
        sim = delete_subdivision(db, simulation_id=simulation_id, subdivision_id=subdivision_id, user_id=cast(int, user.id))
        return SimulationSummary(
            id=cast(UUID, sim.id),
            lake_id=cast(UUID, sim.lake_id),
            dataset_version_id=cast(UUID, sim.dataset_version_id),
            name=cast(str, sim.name),
            status=sim.status,  # type: ignore
            subdivision_count=cast(int, sim.subdivision_count),
            total_selected_cells=cast(int, sim.total_selected_cells),
        )
    except ValueError as e:
        _raise_sim_error(str(e))


@router.get("/lakes/{lake_id}/simulations", response_model=list[SimulationSummary])
def list_sims_for_lake(lake_id: UUID, db: Session = Depends(get_postgis_db), user: User = Depends(get_current_user)):
    sims = list_simulations_by_user_and_lake(db, user_id=cast(int, user.id), lake_id=lake_id)
    return [
        SimulationSummary(
            id=cast(UUID, s.id),
            lake_id=cast(UUID, s.lake_id),
            dataset_version_id=cast(UUID, s.dataset_version_id),
            name=cast(str, s.name),
            status=s.status,  # type: ignore
            subdivision_count=cast(int, s.subdivision_count),
            total_selected_cells=cast(int, s.total_selected_cells),
        )
        for s in sims
    ]
