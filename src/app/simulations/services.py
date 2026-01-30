from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import base64
import zlib
import numpy as np
from sqlalchemy.orm import Session

from app.common.errors import AppError, forbidden
from app.lakes.repository import get_active_dataset_version
from app.lakes.services import validate_and_rasterize_geometry
from app.simulations.models import Simulation, Subdivision, SimulationRun


MAX_SUBDIVISIONS = 100


def _decode_bitset(bitset_b64: str) -> bytes:
    compressed = base64.b64decode(bitset_b64.encode("ascii"))
    return zlib.decompress(compressed)


def _encode_bitset(raw: bytes) -> str:
    return base64.b64encode(zlib.compress(raw, level=6)).decode("ascii")


def _bitset_or(a: str, b: str) -> str:
    ba = _decode_bitset(a)
    bb = _decode_bitset(b)
    if len(ba) != len(bb):
        raise AppError(code="BITSET_DIMENSION_MISMATCH", message="Internal bitset dimension mismatch.", status_code=500)
    out = bytes(x | y for x, y in zip(ba, bb))
    return _encode_bitset(out)


def _bitset_intersects(a: str, b: str) -> int:
    ba = _decode_bitset(a)
    bb = _decode_bitset(b)
    if len(ba) != len(bb):
        raise AppError(code="BITSET_DIMENSION_MISMATCH", message="Internal bitset dimension mismatch.", status_code=500)
    # Count overlapping set bits (slow but OK for <=100 subdivisions; optimize later if needed)
    overlap = 0
    for x, y in zip(ba, bb):
        overlap += int(bin(x & y).count("1"))
    return overlap


def create_simulation(db: Session, *, user_id: UUID, lake_id: UUID, name: str, dataset_version_id: Optional[UUID]) -> Simulation:
    """
    Create an editable simulation with a fixed dataset_version_id.
    """
    if dataset_version_id is None:
        dv = get_active_dataset_version(db, lake_id)
        if dv is None:
            raise AppError(code="NO_ACTIVE_DATASET_VERSION", message="No active dataset version for the specified lake.", status_code=400)
        dataset_version_id = dv.id
        
    sim = Simulation(
        user_id=user_id,
        lake_id=lake_id,
        dataset_version_id=dataset_version_id,
        name=name,
        occupied_bitset_base64=None,
        subdivision_count=0,
        total_selected_cells=0,
        updated_at=datetime.now(timezone.utc),
    )
    db.add(sim)
    db.commit()
    db.refresh(sim)
    return sim


def add_subdivision(
    db: Session,
    *,
    sim: Simulation,
    user_id: UUID,
    geometry: Dict[str, Any],
    geometry_crs: str,
    all_touched: bool,
    inhabitants: int,
    impact_factor: float,
    dataset_version_id: Optional[UUID],
) -> Tuple[Optional[Subdivision], Optional[Dict[str, Any]]]:
    """
    Add a subdivision if valid.
    Returns (subdivision, validation_error_payload).

    UX-friendly behavior:
      - User-drawable validation errors return (None, {...}) instead of raising.
      - Hard failures raise AppError (ownership/resource/internal errors).
    """
    if sim.user_id != user_id:
        raise forbidden("FORBIDDEN", "You do not have access to this simulation.")

    if sim.subdivision_count >= MAX_SUBDIVISIONS:
        # User limit: treat as validation (200 ok=false)
        return None, {
            "code": "MAX_SUBDIVISIONS_EXCEEDED",
            "message": f"Maximum subdivisions per simulation is {MAX_SUBDIVISIONS}.",
            "meta": {"max": MAX_SUBDIVISIONS},
        }

    if dataset_version_id is not None and dataset_version_id != sim.dataset_version_id:
        return None, {
            "code": "DATASET_VERSION_IMMUTABLE",
            "message": "Dataset version does not match simulation dataset_version_id.",
            "meta": {"simulation_dataset_version_id": str(sim.dataset_version_id), "provided": str(dataset_version_id)},
        }

    # Validate geometry against lake raster constraints (water/inhabitants)
    validation = validate_and_rasterize_geometry(
        db,
        lake_id=sim.lake_id,
        geometry_obj=geometry,
        geometry_crs=geometry_crs,
        all_touched=all_touched,
        dataset_version_id=sim.dataset_version_id,
    )

    if not validation.ok:
        # user-correctable: return meta to frontend
        return None, {
            "code": "INVALID_SELECTION",
            "message": "Geometry selection is invalid.",
            "meta": {
                "selected_cells": validation.selected_cells,
                "blocked_cells": validation.blocked_cells,
            },
            "data": validation.model_dump(),
        }

    # Check overlap with existing occupied bitset
    new_bitset = validation.selection_bitset_base64
    assert new_bitset is not None

    if sim.occupied_bitset_base64:
        overlap_cells = _bitset_intersects(sim.occupied_bitset_base64, new_bitset)
        if overlap_cells > 0:
            return None, {
                "code": "SUBDIVISION_OVERLAP",
                "message": "Geometry overlaps an existing subdivision.",
                "meta": {"overlap_cells": overlap_cells},
            }
        sim.occupied_bitset_base64 = _bitset_or(sim.occupied_bitset_base64, new_bitset)
    else:
        sim.occupied_bitset_base64 = new_bitset

    sub = Subdivision(
        simulation_id=sim.id,
        geometry=geometry,
        geometry_crs=geometry_crs,
        all_touched=bool(all_touched),
        selection_bitset_base64=new_bitset,
        selected_cells=validation.selected_cells,
        inhabitants=inhabitants,
        impact_factor=impact_factor,
    )
    db.add(sub)

    sim.subdivision_count += 1
    sim.total_selected_cells += validation.selected_cells
    sim.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(sub)
    db.refresh(sim)

    return sub, None


def delete_subdivision(db: Session, *, sim: Simulation, user_id: UUID, subdivision_id: UUID) -> None:
    if sim.user_id != user_id:
        raise forbidden("FORBIDDEN", "You do not have access to this simulation.")

    # Load subdivision from relationship or query
    target = None
    for s in sim.subdivisions:
        if s.id == subdivision_id:
            target = s
            break
    if target is None:
        raise AppError(code="SUBDIVISION_NOT_FOUND", message="Subdivision not found.", status_code=404)

    db.delete(target)
    db.flush()

    # Recompute occupied bitset safely from remaining subdivisions
    occupied: Optional[str] = None
    total_cells = 0
    for s in sim.subdivisions:
        if s.id == subdivision_id:
            continue
        total_cells += s.selected_cells
        occupied = s.selection_bitset_base64 if occupied is None else _bitset_or(occupied, s.selection_bitset_base64)

    sim.occupied_bitset_base64 = occupied
    sim.subdivision_count = max(0, sim.subdivision_count - 1)
    sim.total_selected_cells = total_cells
    sim.updated_at = datetime.now(timezone.utc)

    db.commit()


def create_run(db: Session, *, sim: Simulation, user_id: UUID) -> SimulationRun:
    """
    Create an immutable run snapshot. Results are stubbed for now.
    When the mathematical engine is ready, compute and fill results_total/results_by_subdivision.
    """
    if sim.user_id != user_id:
        raise forbidden("FORBIDDEN", "You do not have access to this simulation.")

    snapshot = {
        "simulation_id": str(sim.id),
        "lake_id": str(sim.lake_id),
        "dataset_version_id": str(sim.dataset_version_id),
        "name": sim.name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "subdivisions": [
            {
                "id": str(s.id),
                "geometry": s.geometry,
                "geometry_crs": s.geometry_crs,
                "all_touched": bool(s.all_touched),
                "selected_cells": s.selected_cells,
                "inhabitants": s.inhabitants,
                "impact_factor": s.impact_factor,
            }
            for s in sim.subdivisions
        ],
    }

    run = SimulationRun(
        simulation_id=sim.id,
        status="DONE",
        inputs_snapshot=snapshot,
        results_total=None,              # TODO: fill with model outputs
        results_by_subdivision=None,     # TODO: fill with per-subdivision contributions
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run
