import uuid
from sqlalchemy import Column, String, Integer, Numeric, Text, ForeignKey, DateTime, Enum, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from app.postgis_database import PostgisBase

# For dataset statuses
DatasetStatus = Enum("DRAFT", "ACTIVE", "DEPRECATED", name="dataset_status")
# For layer kinds. CI's store contamination coefficients
LayerKind = Enum("WATER", "INHABITANTS", "CI", name="layer_kind")
LayerFormat = Enum("COG", name="layer_format")

class Lake(PostgisBase):
    __tablename__ = "lakes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False, unique=True)

    crs = Column(Text, nullable=False)  # e.g. EPSG:3857
    grid_rows = Column(Integer, nullable=False)
    grid_cols = Column(Integer, nullable=False)
    cell_size_m = Column(Numeric, nullable=False)

    origin_corner = Column(Text, nullable=False, default="top_left")
    origin_x = Column(Numeric, nullable=False)
    origin_y = Column(Numeric, nullable=False)

    extent_geom = Column(Geometry("POLYGON", srid=3857), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    dataset_versions = relationship("LakeDatasetVersion", back_populates="lake", cascade="all, delete-orphan")


class LakeDatasetVersion(PostgisBase):
    __tablename__ = "lake_dataset_versions"
    __table_args__ = (
        UniqueConstraint("lake_id", "version", name="ux_lake_version"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lake_id = Column(UUID(as_uuid=True), ForeignKey("lakes.id", ondelete="CASCADE"), nullable=False)

    version = Column(Integer, nullable=False)
    status = Column(DatasetStatus, nullable=False, default="DRAFT")
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    lake = relationship("Lake", back_populates="dataset_versions")
    layers = relationship("LakeLayer", back_populates="dataset_version", cascade="all, delete-orphan")


class LakeLayer(PostgisBase):
    __tablename__ = "lake_layers"
    __table_args__ = (
        UniqueConstraint("dataset_version_id", "layer_kind", name="ux_dataset_layer_kind"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_version_id = Column(UUID(as_uuid=True), ForeignKey("lake_dataset_versions.id", ondelete="CASCADE"), nullable=False)

    layer_kind = Column(LayerKind, nullable=False)
    format = Column(LayerFormat, nullable=False, default="COG")

    storage_uri = Column(Text, nullable=False)  # s3://bucket/key
    rows = Column(Integer, nullable=False)
    cols = Column(Integer, nullable=False)
    dtype = Column(Text, nullable=False)  # uint8/int32/float32
    nodata = Column(Numeric, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    dataset_version = relationship("LakeDatasetVersion", back_populates="layers")
