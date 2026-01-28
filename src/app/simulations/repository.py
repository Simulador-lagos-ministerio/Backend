"""Repository helpers for simulations persistence."""
from __future__ import annotations

from uuid import UUID
from sqlalchemy.orm import Session

from app.simulations.models import Simulation, Subdivision


def get_simulation(db: Session, simulation_id: UUID) -> Simulation:
    sim = db.query(Simulation).filter(Simulation.id == simulation_id).first()
    if not sim:
        raise ValueError("SIMULATION_NOT_FOUND")
    return sim


def list_subdivisions(db: Session, simulation_id: UUID) -> list[Subdivision]:
    return (
        db.query(Subdivision)
        .filter(Subdivision.simulation_id == simulation_id)
        .order_by(Subdivision.created_at.asc())
        .all())
  

def get_subdivision(db: Session, subdivision_id: UUID, simulation_id: UUID) -> Subdivision:
    sub = (
        db.query(Subdivision)
        .filter(Subdivision.id == subdivision_id)
        .filter(Subdivision.simulation_id == simulation_id)
        .first()
    )
    if not sub:
        raise ValueError("SUBDIVISION_NOT_FOUND")
    return sub

def list_simulations_by_user_and_lake(db: Session, *, user_id: int, lake_id: UUID) -> list[Simulation]:
    return (
        db.query(Simulation)
        .filter(Simulation.user_id == user_id)
        .filter(Simulation.lake_id == lake_id)
        .order_by(Simulation.created_at.desc())
        .all()
    )

