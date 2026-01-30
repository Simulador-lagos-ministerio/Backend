# tests/unit/lakes/test_lakes_repository.py
"""
Unit tests for lakes repository.

We validate:
- get_lake success / not found
- resolve_dataset_version behavior
- get_layer mapping from API kind
- read_layer_array reads band 1 and cleans up tempfiles (indirect)
"""

from __future__ import annotations

import numpy as np
import pytest

from tests._resolve import resolve_lakes_models


def test_get_lake_exists(db, seeded_lake):
    lake_id, _, _ = seeded_lake
    from app.lakes.repository import get_lake  # type: ignore

    lake = get_lake(db, lake_id)
    assert str(lake.id) == lake_id


def test_get_lake_not_found(db):
    from app.lakes.repository import get_lake  # type: ignore
    with pytest.raises(Exception):
        get_lake(db, "00000000-0000-0000-0000-000000000000")


def test_get_active_dataset_version_active(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    from app.lakes.repository import get_active_dataset_version  # type: ignore

    dv = get_active_dataset_version(db, lake_id=lake_id)
    assert str(dv.id) == dv_id


def test_get_active_dataset_version_not_found(db):
    from app.lakes.repository import get_active_dataset_version  # type: ignore

    dv = get_active_dataset_version(db, lake_id="00000000-0000-0000-0000-000000000000")
    assert dv is None


def test_resolve_dataset_version_active(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    from app.lakes.repository import resolve_dataset_version  # type: ignore

    dv = resolve_dataset_version(db, lake_id=lake_id, dataset_version_id=None)
    assert str(dv.id) == dv_id


def test_resolve_dataset_version_not_found(db, seeded_lake):
    lake_id, _, _ = seeded_lake
    from app.lakes.repository import resolve_dataset_version  # type: ignore

    with pytest.raises(Exception):
        resolve_dataset_version(db, lake_id=lake_id, dataset_version_id="00000000-0000-0000-0000-000000000000")
    

def test_get_layer_by_kind(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    from app.lakes.repository import get_layer  # type: ignore

    layer = get_layer(db, dataset_version_id=dv_id, layer_kind_api="water")
    assert layer is not None


def test_get_layer_invalid_kind(db, seeded_lake):
    lake_id, dv_id, _ = seeded_lake
    from app.lakes.repository import get_layer  # type: ignore

    with pytest.raises(Exception):
        get_layer(db, dataset_version_id=dv_id, layer_kind_api="invalid_kind")


def test_read_layer_array_reads_band1(db, seeded_lake):
    _, dv_id, _ = seeded_lake
    from app.lakes.repository import get_layer, read_layer_array  # type: ignore

    layer = get_layer(db, dataset_version_id=dv_id, layer_kind_api="water")
    arr = read_layer_array(layer)
    assert isinstance(arr, np.ndarray)
    assert arr.ndim == 2
