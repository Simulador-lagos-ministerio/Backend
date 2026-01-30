from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Integer, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.postgis_database import Base


class DatasetStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class LayerKind(str, enum.Enum):
    WATER = "WATER"
    INHABITANTS = "INHABITANTS"
    CI = "CI"


class Lake(Base):
    __tablename__ = "lakes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)

    grid_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    grid_cols: Mapped[int] = mapped_column(Integer, nullable=False)
    cell_size_m: Mapped[float] = mapped_column(Float, nullable=False)

    crs: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g. EPSG:3857
    origin_corner: Mapped[str] = mapped_column(String(32), default="top_left", nullable=False)
    origin_x: Mapped[float] = mapped_column(Float, nullable=False)
    origin_y: Mapped[float] = mapped_column(Float, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    datasets: Mapped[list["LakeDatasetVersion"]] = relationship(back_populates="lake", cascade="all, delete-orphan")


class LakeDatasetVersion(Base):
    __tablename__ = "lake_dataset_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lake_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("lakes.id", ondelete="CASCADE"), index=True, nullable=False)

    status: Mapped[str] = mapped_column(String(16), default=DatasetStatus.ACTIVE.value, nullable=False)
    meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    lake: Mapped["Lake"] = relationship(back_populates="datasets")
    layers: Mapped[list["LakeLayer"]] = relationship(back_populates="dataset", cascade="all, delete-orphan")


class LakeLayer(Base):
    __tablename__ = "lake_layers"
    __table_args__ = (
        UniqueConstraint("dataset_version_id", "layer_kind", name="uq_layer_per_dataset_kind"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("lake_dataset_versions.id", ondelete="CASCADE"), index=True, nullable=False)

    layer_kind: Mapped[str] = mapped_column(String(32), nullable=False)  # WATER/INHABITANTS/CI
    storage_uri: Mapped[str] = mapped_column(String(512), nullable=False)  # s3://bucket/key

    rows: Mapped[int] = mapped_column(Integer, nullable=False)
    cols: Mapped[int] = mapped_column(Integer, nullable=False)
    dtype: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    nodata: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    dataset: Mapped["LakeDatasetVersion"] = relationship(back_populates="layers")
