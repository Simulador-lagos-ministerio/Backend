from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.postgis_database import get_db
from app.common.responses import json_ok, json_fail
from app.users.services import oauth2_scheme, get_current_user
from app.simulations.schemas import SimulationCreate, SubdivisionCreate
from app.simulations.repository import get_simulation, get_run
from app.simulations.services import create_simulation, add_subdivision, delete_subdivision, create_run

router = APIRouter()


@router.post("/simulations")
def create_sim(payload: SimulationCreate, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    sim = create_simulation(db, user_id=user.id, lake_id=payload.lake_id, name=payload.name, dataset_version_id=payload.dataset_version_id)
    return json_ok(data={
        "id": str(sim.id),
        "user_id": str(sim.user_id),
        "lake_id": str(sim.lake_id),
        "dataset_version_id": str(sim.dataset_version_id),
        "name": sim.name,
        "subdivision_count": sim.subdivision_count,
        "total_selected_cells": sim.total_selected_cells,
        "occupied_bitset_base64": sim.occupied_bitset_base64,
    })


@router.get("/simulations/{simulation_id}")
def get_sim(simulation_id: UUID, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    sim = get_simulation(db, simulation_id)
    if sim.user_id != user.id:
        # Hard access error
        from app.common.errors import forbidden
        raise forbidden("FORBIDDEN", "You do not have access to this simulation.")

    # Ensure subdivisions are loaded
    subs = []
    for s in sim.subdivisions:
        subs.append({
            "id": str(s.id),
            "simulation_id": str(s.simulation_id),
            "geometry": s.geometry,
            "geometry_crs": s.geometry_crs,
            "all_touched": bool(s.all_touched),
            "selected_cells": s.selected_cells,
            "selection_bitset_base64": s.selection_bitset_base64,
            "inhabitants": s.inhabitants,
            "impact_factor": s.impact_factor,
        })

    return json_ok(data={
        "id": str(sim.id),
        "user_id": str(sim.user_id),
        "lake_id": str(sim.lake_id),
        "dataset_version_id": str(sim.dataset_version_id),
        "name": sim.name,
        "subdivision_count": sim.subdivision_count,
        "total_selected_cells": sim.total_selected_cells,
        "occupied_bitset_base64": sim.occupied_bitset_base64,
        "subdivisions": subs,
    })


@router.post("/simulations/{simulation_id}/subdivisions")
def post_subdivision(simulation_id: UUID, payload: SubdivisionCreate, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    sim = get_simulation(db, simulation_id)

    sub, validation_error = add_subdivision(
        db,
        sim=sim,
        user_id=user.id,
        geometry=payload.geometry,
        geometry_crs=payload.geometry_crs,
        all_touched=payload.all_touched,
        inhabitants=payload.inhabitants,
        impact_factor=payload.impact_factor,
        dataset_version_id=payload.dataset_version_id,
    )

    if validation_error:
        return json_fail(
            code=validation_error["code"],
            message=validation_error["message"],
            meta=validation_error.get("meta", {}),
            data=validation_error.get("data"),
            status_code=200,
        )

    assert sub is not None
    return json_ok(data={
        "id": str(sub.id),
        "simulation_id": str(sub.simulation_id),
        "geometry": sub.geometry,
        "geometry_crs": sub.geometry_crs,
        "all_touched": bool(sub.all_touched),
        "selected_cells": sub.selected_cells,
        "selection_bitset_base64": sub.selection_bitset_base64,
        "inhabitants": sub.inhabitants,
        "impact_factor": sub.impact_factor,
    })


@router.delete("/simulations/{simulation_id}/subdivisions/{subdivision_id}")
def delete_sub(simulation_id: UUID, subdivision_id: UUID, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    sim = get_simulation(db, simulation_id)
    delete_subdivision(db, sim=sim, user_id=user.id, subdivision_id=subdivision_id)
    return json_ok(data={"deleted": True})


@router.post("/simulations/{simulation_id}/runs")
def run_simulation(simulation_id: UUID, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    sim = get_simulation(db, simulation_id)
    run = create_run(db, sim=sim, user_id=user.id)
    return json_ok(data={
        "id": str(run.id),
        "simulation_id": str(run.simulation_id),
        "status": run.status,
        "created_at": run.created_at.isoformat(),
        "inputs_snapshot": run.inputs_snapshot,
        "results_total": run.results_total,
        "results_by_subdivision": run.results_by_subdivision,
    })


@router.get("/runs/{run_id}")
def get_run_by_id(run_id: UUID, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    user = get_current_user(db, token)
    run = get_run(db, run_id)
    # Ownership check via simulation relationship would require join; simplest:
    sim = get_simulation(db, run.simulation_id)
    if sim.user_id != user.id:
        from app.common.errors import forbidden
        raise forbidden("FORBIDDEN", "You do not have access to this run.")
    return json_ok(data={
        "id": str(run.id),
        "simulation_id": str(run.simulation_id),
        "status": run.status,
        "created_at": run.created_at.isoformat(),
        "inputs_snapshot": run.inputs_snapshot,
        "results_total": run.results_total,
        "results_by_subdivision": run.results_by_subdivision,
    })


@router.post("/simulations/{simulation_id}/finalize")
def finalize_alias(simulation_id: UUID, db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    """
    Backward-compatible alias: finalize == create a run.
    """
    return run_simulation(simulation_id, db, token)
