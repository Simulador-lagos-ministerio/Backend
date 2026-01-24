import json
import uuid
from pathlib import Path

from sqlalchemy.orm import Session

from app.postgis_database import PostgisSessionLocal
from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer


# Ajustá a tus keys reales en MinIO (bucket maps).
S3_BASE = "s3://maps/lakes/san_roque_demo/v1"  


def main():


    meta = {
  "dataset_name": "san_roque_demo_v1",
  "purpose": "Synthetic demo layers for backend testing (NOT production)",
  "created_utc": "2026-01-24T00:17:36.891767Z",
  "grid": {
    "rows": 300,
    "cols": 300,
    "cell_size_m": 100.0,
    "crs": "EPSG:3857",
    "origin_corner": "top_left",
    "origin_x": -7191767.571442347,
    "origin_y": -3665894.488587622,
    "center_lonlat": {
      "lon": -64.47,
      "lat": -31.37
    }
  },
  "layers": {
    "water": {
      "file": "water.tif",
      "dtype": "uint8",
      "meaning": "0=land, 1=water",
      "generation": "Ellipse mask near right side within synthetic domain"
    },
    "inhabitants": {
      "file": "inhabitants.tif",
      "dtype": "int32",
      "meaning": "inhabitants per cell (base)",
      "generation": "Two gaussian clusters + sparse rural noise; zeroed on water/outside domain"
    },
    "ci": {
      "file": "ci.tif",
      "dtype": "float32",
      "meaning": "attenuation coefficient proxy in [0,1]",
      "generation": "exp(-dist_to_water/5000m) using BFS 4-neighborhood distance; zeroed on water/outside domain",
      "parameters": {
        "k": 0.0002,
        "attenuation_length_m": 5000
      }
    }
  },
  "validation": {
    "water_count": 3343,
    "inhabited_cells_count": 11829,
    "inhabitants_total": 179403,
    "ci_min": 0.0,
    "ci_max": 0.9801986813545227
  }
}
    grid = meta["grid"]

    lake_id = uuid.uuid4()

    db: Session = PostgisSessionLocal()

    # 1) Crear Lake
    lake = Lake(
        id=lake_id,
        name="Lago San Roque (DEMO)",
        crs=grid["crs"],                 # "EPSG:3857"
        grid_rows=grid["rows"],          # 300
        grid_cols=grid["cols"],          # 300
        cell_size_m=grid["cell_size_m"], # 100.0
        origin_corner=grid["origin_corner"],  # "top_left"
        origin_x=grid["origin_x"],
        origin_y=grid["origin_y"],
        extent_geom=None,  # demo: omitimos; en prod podrías guardar polígono
    )
    db.add(lake)
    db.flush()

    # 2) Crear DatasetVersion ACTIVE
    dv = LakeDatasetVersion(
        lake_id=lake_id,
        version=1,
        status="ACTIVE",
        notes="Demo v1 - capas sintéticas para test backend"
    )
    db.add(dv)
    db.flush()

    # 3) Crear Layers (WATER / INHABITANTS / CI)
    layers = [
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="WATER",
            format="COG",
            storage_uri=f"{S3_BASE}/water.tif",
            rows=grid["rows"],
            cols=grid["cols"],
            dtype=meta["layers"]["water"]["dtype"],  # "uint8"
            nodata=0,
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="INHABITANTS",
            format="COG",
            storage_uri=f"{S3_BASE}/inhabitants.tif",
            rows=grid["rows"],
            cols=grid["cols"],
            dtype=meta["layers"]["inhabitants"]["dtype"],  # "int32"
            nodata=0,
        ),
        LakeLayer(
            dataset_version_id=dv.id,
            layer_kind="CI",
            format="COG",
            storage_uri=f"{S3_BASE}/ci.tif",
            rows=grid["rows"],
            cols=grid["cols"],
            dtype=meta["layers"]["ci"]["dtype"],  # "float32"
            nodata=0.0,
        ),
    ]

    db.add_all(layers)
    db.commit()

    print("Seed OK")
    print("lake_id:", lake_id)
    print("dataset_version_id:", dv.id)
    print("S3_BASE:", S3_BASE)


if __name__ == "__main__":
    main()
