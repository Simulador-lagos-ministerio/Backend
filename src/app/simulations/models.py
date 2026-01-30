from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.postgis_database import Base


class Simulation(Base):
    __tablename__ = "simulations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    lake_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("lakes.id", ondelete="CASCADE"), index=True, nullable=False)
    dataset_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("lake_dataset_versions.id", ondelete="RESTRICT"), index=True, nullable=False)

    name: Mapped[str] = mapped_column(String(200), nullable=False)

    # Union of all subdivision selections
    occupied_bitset_base64: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    subdivision_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_selected_cells: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    subdivisions: Mapped[list["Subdivision"]] = relationship(back_populates="simulation", cascade="all, delete-orphan")
    runs: Mapped[list["SimulationRun"]] = relationship(back_populates="simulation", cascade="all, delete-orphan")


class Subdivision(Base):
    __tablename__ = "subdivisions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    simulation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("simulations.id", ondelete="CASCADE"), index=True, nullable=False)

    # Persist original geometry for frontend rehydration
    geometry: Mapped[dict] = mapped_column(JSONB, nullable=False)
    geometry_crs: Mapped[str] = mapped_column(String(64), nullable=False, default="EPSG:4326")
    all_touched: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)  

    selection_bitset_base64: Mapped[str] = mapped_column(String, nullable=False)
    selected_cells: Mapped[int] = mapped_column(Integer, nullable=False)

    # User inputs per subdivision
    inhabitants: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    impact_factor: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    simulation: Mapped["Simulation"] = relationship(back_populates="subdivisions")


class SimulationRun(Base):
    __tablename__ = "simulation_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    simulation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("simulations.id", ondelete="CASCADE"), index=True, nullable=False)

    status: Mapped[str] = mapped_column(String(16), default="DONE", nullable=False)  # PENDING|DONE|FAILED

    # Snapshot of the simulation inputs at run-time (reproducibility)
    inputs_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Results placeholders (fill when model is implemented)
    results_total: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    results_by_subdivision: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    simulation: Mapped["Simulation"] = relationship(back_populates="runs")
