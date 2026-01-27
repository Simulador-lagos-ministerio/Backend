import base64
import zlib

import numpy as np

from app.lakes.geometry_services import mask_to_bitset_bytes, encode_bitset_zlib_base64


def _decode_zlib_base64(b64: str) -> bytes:
    return zlib.decompress(base64.b64decode(b64.encode("ascii")))


def test_mask_to_bitset_size_ceil_bytes():
    # 20*20 = 400 bits -> 50 bytes exactos
    mask = np.zeros((20, 20), dtype=bool)
    b = mask_to_bitset_bytes(mask)
    assert isinstance(b, (bytes, bytearray))
    assert len(b) == 50


def test_mask_to_bitset_lsb0_single_byte():
    # 1x8: primer bit True -> en lsb0 el byte debe ser 0b00000001 == 1
    mask = np.array([[True, False, False, False, False, False, False, False]], dtype=bool)
    b = mask_to_bitset_bytes(mask)
    assert len(b) == 1
    assert b[0] == 1  # lsb0

    # Si fuese msb0 sería 128; esto asegura el contrato "lsb0".


def test_mask_to_bitset_crosses_byte_boundary():
    # 1x9: el 9no bit cae en el segundo byte bit0 -> bytes esperados: [0,1]
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
