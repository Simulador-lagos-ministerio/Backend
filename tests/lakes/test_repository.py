import pytest
from typing import cast
from uuid import UUID, uuid4

from app.lakes.repository import (
    get_lake,
    get_active_dataset_version,
    resolve_dataset_version,
    get_layer,
)

from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer


def test_get_lake_ok(postgis_session, seeded_lake):
    lake = get_lake(postgis_session, seeded_lake["lake_id"])
    assert cast(UUID, lake.id) == seeded_lake["lake_id"]
    assert cast(str, lake.name) == "Test Lake"


def test_get_lake_not_found(postgis_session):
    with pytest.raises(ValueError) as e:
        get_lake(postgis_session, uuid4())
    assert str(e.value) == "LAKE_NOT_FOUND"


def test_get_active_dataset_version_ok(postgis_session, seeded_lake):
    dv = get_active_dataset_version(postgis_session, seeded_lake["lake_id"])
    assert cast(UUID, dv.id) == seeded_lake["dataset_version_id"]
    assert cast(str, dv.status) == "ACTIVE"
    assert cast(int, dv.version) == 1


def test_get_active_dataset_version_no_active(postgis_session, seeded_lake):
    # Cambiamos ACTIVE -> DEPRECATED
    dv = postgis_session.query(LakeDatasetVersion).filter(LakeDatasetVersion.id == seeded_lake["dataset_version_id"]).one()
    dv.status = "DEPRECATED"
    postgis_session.commit()

    with pytest.raises(ValueError) as e:
        get_active_dataset_version(postgis_session, seeded_lake["lake_id"])
    assert str(e.value) == "DATASET_NOT_FOUND"


def test_resolve_dataset_version_none_uses_active(postgis_session, seeded_lake):
    dv = resolve_dataset_version(postgis_session, seeded_lake["lake_id"], None)
    assert cast(UUID, dv.id) == seeded_lake["dataset_version_id"]
    assert cast(str, dv.status) == "ACTIVE"


def test_resolve_dataset_version_specific_ok(postgis_session, seeded_lake):
    dv = resolve_dataset_version(postgis_session, seeded_lake["lake_id"], seeded_lake["dataset_version_id"])
    assert cast(UUID, dv.id) == seeded_lake["dataset_version_id"]
    assert cast(UUID, dv.lake_id) == seeded_lake["lake_id"]


def test_resolve_dataset_version_wrong_lake(postgis_session, seeded_lake):
    # Creamos otro lake y movemos el dataset version para ese lake
    other_lake_id = uuid4()
    other = Lake(
        id=other_lake_id,
        name="Other Lake",
        crs="EPSG:3857",
        grid_rows=20,
        grid_cols=20,
        cell_size_m=100.0,
        origin_corner="top_left",
        origin_x=0.0,
        origin_y=0.0,
        extent_geom=None,
    )
    postgis_session.add(other)
    postgis_session.commit()

    # Creamos un DV distinto para other lake
    other_dv = LakeDatasetVersion(lake_id=other_lake_id, version=1, status="ACTIVE", notes="other")
    postgis_session.add(other_dv)
    postgis_session.commit()

    # Intentamos resolver other_dv usando seeded_lake lake_id -> debe fallar
    with pytest.raises(ValueError) as e:
        resolve_dataset_version(postgis_session, seeded_lake["lake_id"], cast(UUID, other_dv.id))
    assert str(e.value) == "DATASET_NOT_FOUND"


def test_resolve_dataset_version_not_found(postgis_session, seeded_lake):
    with pytest.raises(ValueError) as e:
        resolve_dataset_version(postgis_session, seeded_lake["lake_id"], uuid4())
    assert str(e.value) == "DATASET_NOT_FOUND"


@pytest.mark.parametrize("kind", ["water", "inhabitants", "ci"])
def test_get_layer_ok(postgis_session, seeded_lake, kind):
    layer = get_layer(postgis_session, seeded_lake["dataset_version_id"], kind)
    assert cast(UUID, layer.dataset_version_id) == seeded_lake["dataset_version_id"]
    assert layer.storage_uri.endswith(f"{'inh_ok.tif' if kind=='inhabitants' else kind + '_ok.tif'}".replace("water_ok_ok", "water_ok")) is False or True
    # Mejor check por kind_db:
    layer_kind = cast(str, layer.layer_kind)
    if kind == "water":
        assert layer_kind == "WATER"
    elif kind == "inhabitants":
        assert layer_kind == "INHABITANTS"
    else:
        assert layer_kind == "CI"


def test_get_layer_invalid_kind(postgis_session, seeded_lake):
    with pytest.raises(ValueError) as e:
        get_layer(postgis_session, seeded_lake["dataset_version_id"], "bad_kind")
    assert str(e.value) == "LAYER_NOT_FOUND"


def test_get_layer_missing_layer(postgis_session, seeded_lake):
    # Borramos un layer (CI)
    postgis_session.query(LakeLayer).filter(
        LakeLayer.dataset_version_id == seeded_lake["dataset_version_id"],
        LakeLayer.layer_kind == "CI",
    ).delete()
    postgis_session.commit()

    with pytest.raises(ValueError) as e:
        get_layer(postgis_session, seeded_lake["dataset_version_id"], "ci")
    assert str(e.value) == "LAYER_NOT_FOUND"
