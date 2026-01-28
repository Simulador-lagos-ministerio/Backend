"""Domain services for simulations: validation, overlap checks, persistence updates."""
from __future__ import annotations

from typing import cast
from unittest import result
from uuid import UUID, uuid4
import numpy as np
from sqlalchemy.orm import Session

from app.lakes.repository import get_lake, resolve_dataset_version
from app.lakes.geometry_services import (
    ENCODING, BIT_ORDER, CELL_ORDER,
    mask_to_bitset_bytes,
    encode_bitset_zlib_base64,
    decode_bitset_zlib_base64,
)
from app.lakes.services import validate_and_rasterize_geometry

from app.simulations.models import Simulation, Subdivision
from app.simulations.repository import get_simulation, get_subdivision, list_subdivisions


MAX_SUBDIVISIONS_PER_SIMULATION = 100


def _bytes_intersects(a: bytes, b: bytes) -> bool:
    """Return True if any bit is set in (a AND b)."""
    if not a or not b:
        return False
    aa = np.frombuffer(a, dtype=np.uint8)
    bb = np.frombuffer(b, dtype=np.uint8)
    if aa.shape != bb.shape:
        # Defensive: dimensions mismatch should never happen if rows/cols are consistent.
        raise ValueError("BITSET_DIMENSION_MISMATCH")
    return bool(np.any(np.bitwise_and(aa, bb)))


def _bytes_or(a: bytes, b: bytes) -> bytes:
    """Return (a OR b) as bytes."""
    if not a:
        return b
    if not b:
        return a
    aa = np.frombuffer(a, dtype=np.uint8)
    bb = np.frombuffer(b, dtype=np.uint8)
    if aa.shape != bb.shape:
        raise ValueError("BITSET_DIMENSION_MISMATCH")
    out = np.bitwise_or(aa, bb).astype(np.uint8)
    return out.tobytes()


def create_simulation(db: Session, *, user_id: int, lake_id: UUID, dataset_version_id: UUID, name: str) -> Simulation:
    """Create a draft simulation bound to a single lake and dataset version."""
    lake = get_lake(db, lake_id)
    dv = resolve_dataset_version(db, lake_id, dataset_version_id)

    sim = Simulation(
        id=uuid4(),
        user_id=user_id,
        lake_id=lake_id,
        dataset_version_id=dv.id,
        name=name,
        status="DRAFT",
        rows=int(lake.grid_rows),
        cols=int(lake.grid_cols),
        encoding=ENCODING,
        bit_order=BIT_ORDER,
        cell_order=CELL_ORDER,
        occupied_bitset_base64=None,
        subdivision_count=0,
        total_selected_cells=0,
    )
    db.add(sim)
    db.commit()
    db.refresh(sim)
    return sim


def add_subdivision(
    db: Session,
    simulation_id: UUID,
    user_id: int,
    geometry_geojson: dict,
    geometry_crs: str,
    all_touched: bool,
    dataset_version_id: UUID | None = None,
) -> Subdivision:
    """
    Validate and persist a new subdivision into a draft simulation.

    Validations:
    - ownership
    - simulation status DRAFT
    - max 100 subdivisions
    - polygon-only (delegated to parse_geojson_geometry)
    - not empty selection
    - no intersects with water/inhabitants
    - no overlap with existing subdivisions (cell-level)
    """
    sim = get_simulation(db, simulation_id)

    if cast(bool, sim.user_id != user_id):
        raise ValueError("FORBIDDEN")

    if cast(bool, sim.status != "DRAFT"):
        raise ValueError("SIMULATION_FINALIZED")

    if cast(bool, sim.subdivision_count >= MAX_SUBDIVISIONS_PER_SIMULATION):
        raise ValueError("MAX_SUBDIVISIONS_EXCEEDED")

    # Use simulation.dataset_version_id unless explicitly provided (should match anyway).
    dv_id = dataset_version_id or sim.dataset_version_id
    if cast(bool, dv_id != sim.dataset_version_id):
        raise ValueError("DATASET_VERSION_IMMUTABLE")

    # Reuse the existing lake geometry validation service.
    result = validate_and_rasterize_geometry(
        db=db,
        lake_id=cast(UUID, sim.lake_id),
        dataset_version_id=cast(UUID, sim.dataset_version_id),
        geometry_geojson=geometry_geojson,
        geometry_crs=geometry_crs,
        all_touched=all_touched,
    )

    if "code" in result:
        # Pass through service-level domain codes (LAKE_NOT_FOUND, DATASET_NOT_FOUND, INVALID_GEOJSON, etc).
        raise ValueError(result["code"])

    if not result["ok"]:
        # This means intersects water/inh or other invalid selection.
        raise ValueError("INVALID_SELECTION")

    mask = result["selection_mask"].astype(bool)
    selected_cells = int(result["selected_cells"])
    if selected_cells <= 0:
        raise ValueError("EMPTY_SELECTION")

    # Build raw bitset bytes directly from the mask (no encode->decode roundtrip).
    selection_bytes = mask_to_bitset_bytes(mask)
    selection_b64 = encode_bitset_zlib_base64(selection_bytes, level=6)

    # Overlap check against simulation occupied bitset (stored as compressed base64).
    if cast(bool, sim.occupied_bitset_base64):
        occupied_bytes = decode_bitset_zlib_base64(cast(str, sim.occupied_bitset_base64))
        if _bytes_intersects(occupied_bytes, selection_bytes):
            raise ValueError("SUBDIVISION_OVERLAP")
        union_bytes = _bytes_or(occupied_bytes, selection_bytes)
    else:
        union_bytes = selection_bytes

    # Store union back without reconstructing the mask.
    union_b64 = encode_bitset_zlib_base64(union_bytes, level=6)

    sub = Subdivision(
        id=uuid4(),
        simulation_id=sim.id,
        geometry=geometry_geojson,
        geometry_crs=geometry_crs,
        all_touched=all_touched,
        selection_bitset_base64=selection_b64,
        selected_cells=selected_cells,
    )
    db.add(sub)

    # Update simulation aggregates.
    sim.occupied_bitset_base64 = union_b64
    sim.subdivision_count = int(sim.subdivision_count) + 1
    sim.total_selected_cells = int(sim.total_selected_cells) + int(selected_cells)

    db.commit()
    db.refresh(sub)
    return sub


def delete_subdivision(db: Session, *, simulation_id: UUID, subdivision_id: UUID, user_id: int) -> Simulation:
    """
    Delete a subdivision from a draft simulation and recompute occupied bitset + aggregates.
    """
    sim = get_simulation(db, simulation_id)

    if cast(bool, sim.user_id != user_id):
        raise ValueError("FORBIDDEN")
    if cast(bool, sim.status != "DRAFT"):
        raise ValueError("SIMULATION_FINALIZED")

    sub = get_subdivision(db, subdivision_id, simulation_id)

    db.delete(sub)
    db.flush()

    # Recompute union from remaining subdivisions
    subs = list_subdivisions(db, simulation_id)
    
    union_bytes: bytes | None = None
    total_cells = 0

    for s in subs:
        b = decode_bitset_zlib_base64(cast(str, s.selection_bitset_base64))
        union_bytes = b if union_bytes is None else _bytes_or(union_bytes, b)
        total_cells += int(cast(int, s.selected_cells))

    sim.subdivision_count = len(subs)
    sim.total_selected_cells = int(total_cells)
    sim.occupied_bitset_base64 = encode_bitset_zlib_base64(union_bytes, level=6) if union_bytes else None

    db.commit()
    db.refresh(sim)
    return sim


def finalize_simulation(db: Session, *, simulation_id: UUID, user_id: int) -> Simulation:
    """Lock the simulation so no more subdivisions can be added."""
    sim = get_simulation(db, simulation_id)
    if cast(bool, sim.user_id != user_id):
        raise ValueError("FORBIDDEN")
    if cast(bool, sim.status != "DRAFT"):
        return sim
    sim.status = "FINALIZED"
    db.commit()
    db.refresh(sim)
    return sim
