from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.common.errors import not_found
from app.simulations.models import Simulation, Subdivision, SimulationRun


def get_simulation(db: Session, simulation_id: UUID) -> Simulation:
    sim = db.execute(select(Simulation).where(Simulation.id == simulation_id)).scalar_one_or_none()
    if not sim:
        raise not_found("SIMULATION_NOT_FOUND", "Simulation not found.", {"simulation_id": str(simulation_id)})
    return sim


def get_subdivision(db: Session, subdivision_id: UUID) -> Subdivision:
    sub = db.execute(select(Subdivision).where(Subdivision.id == subdivision_id)).scalar_one_or_none()
    if not sub:
        raise not_found("SUBDIVISION_NOT_FOUND", "Subdivision not found.", {"subdivision_id": str(subdivision_id)})
    return sub


def get_run(db: Session, run_id: UUID) -> SimulationRun:
    run = db.execute(select(SimulationRun).where(SimulationRun.id == run_id)).scalar_one_or_none()
    if not run:
        raise not_found("RUN_NOT_FOUND", "Run not found.", {"run_id": str(run_id)})
    return run
