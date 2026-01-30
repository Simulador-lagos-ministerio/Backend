# tests/_resolve.py
"""
Centralized symbol resolution for tests.

Adjust ONLY this file if your project paths/names differ.

The goal:
- keep test files stable even if you refactor internal module paths.
"""

from __future__ import annotations

from typing import Any, Callable, Tuple, Type


def resolve_app() -> Any:
    """
    Return the FastAPI app instance.
    """
    # Most common:
    from app.main import app  # type: ignore
    return app


def resolve_db_symbols() -> Tuple[Any, Callable]:
    """
    Return (Base, get_db_dependency).
    """
    # Preferred "unified Postgres" layout:
    try:
        from app.database import Base, get_db  # type: ignore
        return Base, get_db
    except Exception:
        pass

    # Fallback: older naming in some refactors
    try:
        from app.postgis_database import PostgisBase as Base  # type: ignore
        from app.postgis_database import get_db  # type: ignore
        return Base, get_db
    except Exception:
        pass

    try:
        from app.postgis_database import Base, get_db  # type: ignore
        return Base, get_db
    except Exception:
        pass

    raise RuntimeError("Could not resolve (Base, get_db). Update tests/_resolve.py.")


def resolve_lakes_models() -> Tuple[Type, Type, Type]:
    """
    Return (Lake, LakeDatasetVersion, LakeLayer) models.
    """
    from app.lakes.models import Lake, LakeDatasetVersion, LakeLayer  # type: ignore
    return Lake, LakeDatasetVersion, LakeLayer


def resolve_lakes_services() -> Any:
    """
    Return lakes services module (for compute/stats/validate functions).
    """
    import app.lakes.services as services  # type: ignore
    return services


def resolve_simulations_services() -> Any:
    """
    Return simulations services module.
    """
    import app.simulations.services as services  # type: ignore
    return services


def resolve_users_services() -> Any:
    """
    Return users services module.
    """
    import app.users.services as services  # type: ignore
    return services


def resolve_responses() -> Tuple[Callable, Callable]:
    """
    Return (json_ok, json_fail) callables.

    If your users API is "raw" (no envelope), these are still required
    for domain endpoints (lakes/simulations).
    """
    from app.common.responses import json_ok, json_fail  # type: ignore
    return json_ok, json_fail
