# tests/unit/lakes/test_geometry_services_bitset.py
"""
Unit tests for geometry_services bitset encoding.

We enforce:
- constants exist (BIT_ORDER, CELL_ORDER)
- encode -> decode roundtrip preserves mask bits
"""

from __future__ import annotations

import numpy as np
import pytest

import app.lakes.geometry_services as gs  # type: ignore

from tests._helpers import decode_mask_from_bitset_b64


def test_geometry_services_public_constants_exist():
    assert hasattr(gs, "BIT_ORDER"), "BIT_ORDER must be defined"
    assert hasattr(gs, "CELL_ORDER"), "CELL_ORDER must be defined"
    assert hasattr(gs, "ENCODING"), "ENCODING must be defined"


def test_bitset_roundtrip_small_mask():
    assert hasattr(gs, "mask_to_encoded_bitset"), "mask_to_encoded_bitset must exist"

    mask = np.zeros((5, 5), dtype=bool)
    mask[0, 0] = True
    mask[2, 3] = True
    mask[4, 4] = True

    encoded = gs.mask_to_encoded_bitset(mask)
    assert isinstance(encoded, str) and len(encoded) > 0

    decoded = decode_mask_from_bitset_b64(encoded, 5, 5)
    assert decoded.shape == mask.shape
    assert np.array_equal(decoded, mask)


def test_bitset_byte_length_matches_cells():
    """
    Packed bytes length should be ceil(ncells/8) after decompress.
    """
    import base64, zlib  # noqa: E401

    mask = np.zeros((7, 9), dtype=bool)
    encoded = gs.mask_to_encoded_bitset(mask)

    raw = zlib.decompress(base64.b64decode(encoded.encode("ascii")))
    n = 7 * 9
    expected = (n + 7) // 8
    assert len(raw) == expected
