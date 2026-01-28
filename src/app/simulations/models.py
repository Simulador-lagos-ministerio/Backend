"""SQLAlchemy models for simulations and subdivisions (PostGIS DB)."""
from __future__ import annotations

import sqlalchemy as _sql
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB

from app.postgis_database import PostgisBase


class Simulation(PostgisBase):
    __tablename__ = "simulations"

    id = _sql.Column(PG_UUID(as_uuid=True), primary_key=True, nullable=False)
    user_id = _sql.Column(_sql.Integer, nullable=False, index=True)  # SQLite user id (no FK)
    lake_id = _sql.Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    dataset_version_id = _sql.Column(PG_UUID(as_uuid=True), nullable=False, index=True)

    name = _sql.Column(_sql.String(255), nullable=False)
    status = _sql.Column(_sql.String(32), nullable=False, default="DRAFT")  # DRAFT|FINALIZED

    rows = _sql.Column(_sql.Integer, nullable=False)
    cols = _sql.Column(_sql.Integer, nullable=False)

    encoding = _sql.Column(_sql.String(64), nullable=False)    # e.g. "bitset+zlib+base64"
    bit_order = _sql.Column(_sql.String(16), nullable=False)   # e.g. "lsb0"
    cell_order = _sql.Column(_sql.String(32), nullable=False)  # e.g. "row_major_cell_id"

    # Union mask of all saved subdivisions for fast overlap checking (compressed b64)
    occupied_bitset_base64 = _sql.Column(_sql.Text, nullable=True)

    subdivision_count = _sql.Column(_sql.Integer, nullable=False, default=0)
    total_selected_cells = _sql.Column(_sql.Integer, nullable=False, default=0)

    created_at = _sql.Column(_sql.DateTime(timezone=True), server_default=_sql.func.now(), nullable=False)
    updated_at = _sql.Column(
        _sql.DateTime(timezone=True),
        server_default=_sql.func.now(),
        onupdate=_sql.func.now(),
        nullable=False,
    )


class Subdivision(PostgisBase):
    __tablename__ = "subdivisions"

    id = _sql.Column(PG_UUID(as_uuid=True), primary_key=True, nullable=False)
    simulation_id = _sql.Column(PG_UUID(as_uuid=True), _sql.ForeignKey("simulations.id", ondelete="CASCADE"), nullable=False, index=True)

    # Store what the user drew (GeoJSON geometry object) + CRS metadata
    geometry = _sql.Column(JSONB, nullable=False)
    geometry_crs = _sql.Column(_sql.String(64), nullable=False)
    all_touched = _sql.Column(_sql.Boolean, nullable=False, default=False)

    selection_bitset_base64 = _sql.Column(_sql.Text, nullable=False)
    selected_cells = _sql.Column(_sql.Integer, nullable=False)

    created_at = _sql.Column(_sql.DateTime(timezone=True), server_default=_sql.func.now(), nullable=False)
