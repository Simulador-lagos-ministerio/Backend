"""Unit tests for bitset encoding helpers."""
import base64
import zlib

import numpy as np

from app.lakes.geometry_services import encode_bitset_zlib_base64, mask_to_bitset_bytes


def _decode_zlib_base64(b64: str) -> bytes:
    return zlib.decompress(base64.b64decode(b64.encode("ascii")))


def test_mask_to_bitset_size_ceil_bytes():
    # 20*20 = 400 bits -> 50 bytes exactly.
    mask = np.zeros((20, 20), dtype=bool)
    b = mask_to_bitset_bytes(mask)
    assert isinstance(b, (bytes, bytearray))
    assert len(b) == 50


def test_mask_to_bitset_lsb0_single_byte():
    # 1x8: first bit True -> LSB0 byte must be 0b00000001 == 1.
    mask = np.array([[True, False, False, False, False, False, False, False]], dtype=bool)
    b = mask_to_bitset_bytes(mask)
    assert len(b) == 1
    assert b[0] == 1  # lsb0

    # If it were MSB0 it would be 128; this asserts the "lsb0" contract.


def test_mask_to_bitset_crosses_byte_boundary():
    # 1x9: the 9th bit lands in the second byte bit0 -> expected bytes: [0, 1].
    mask = np.array([[False] * 8 + [True]], dtype=bool)
    b = mask_to_bitset_bytes(mask)
    assert len(b) == 2
    assert b[0] == 0
    assert b[1] == 1


def test_encode_bitset_zlib_base64_roundtrip():
    mask = np.zeros((4, 4), dtype=bool)
    mask[0, 0] = True
    mask[3, 3] = True

    raw = mask_to_bitset_bytes(mask)
    b64 = encode_bitset_zlib_base64(raw)

    assert isinstance(b64, str)
    decoded = _decode_zlib_base64(b64)
    assert decoded == raw
