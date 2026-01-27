"""Generate fixture rasters for tests (run manually)."""
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin


def write_tif(path: Path, arr: np.ndarray, dtype: str, nodata=None):
    path.parent.mkdir(parents=True, exist_ok=True)

    h, w = arr.shape
    transform = from_origin(0.0, 0.0, 1.0, 1.0)  # irrelevant for stats/mask tests
    profile = {
        "driver": "GTiff",
        "height": h,
        "width": w,
        "count": 1,
        "dtype": dtype,
        "crs": None,
        "transform": transform,
        "compress": "deflate",
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr.astype(dtype), 1)


def main():
    out_dir = Path(__file__).parent / "rasters"

    # OK grid: 20x20
    rows, cols = 20, 20

    # water: uint8 (1 = water, 0 = land)
    water = np.zeros((rows, cols), dtype=np.uint8)
    water[0:3, 0:5] = 1  # small block
    water[10, 10] = 1

    # inhabitants: int32 (>=1 inhabited)
    inh = np.zeros((rows, cols), dtype=np.int32)
    inh[5, 5] = 10
    inh[6, 5] = 3
    inh[15, 2] = 1

    # ci: float32
    ci = np.zeros((rows, cols), dtype=np.float32)
    ci[ci == 0] = 0.2
    ci[0:2, 0:2] = 0.9

    write_tif(out_dir / "water_ok.tif", water, "uint8", nodata=0)
    write_tif(out_dir / "inh_ok.tif", inh, "int32", nodata=0)
    write_tif(out_dir / "ci_ok.tif", ci, "float32", nodata=0.0)

    # Mismatch: 10x10
    water2 = np.zeros((10, 10), dtype=np.uint8)
    inh2 = np.zeros((10, 10), dtype=np.int32)
    ci2 = np.zeros((10, 10), dtype=np.float32)

    write_tif(out_dir / "water_mismatch.tif", water2, "uint8", nodata=0)
    write_tif(out_dir / "inh_mismatch.tif", inh2, "int32", nodata=0)
    write_tif(out_dir / "ci_mismatch.tif", ci2, "float32", nodata=0.0)

    print("OK: rasters created in", out_dir)


if __name__ == "__main__":
    main()
