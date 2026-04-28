from __future__ import annotations

from pathlib import Path

import pytest

from bitswarm.protocol.errors import PieceVerificationError
from bitswarm.protocol.manifest import create_manifest
from bitswarm.protocol.pieces import read_piece
from bitswarm.protocol.verifier import verify_piece_bytes


def test_piece_bytes_verify(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=8)
    piece = manifest.pieces[0]
    data = read_piece(sample_tree, piece)
    verify_piece_bytes(data, piece)


def test_corrupt_piece_is_rejected(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=8)
    piece = manifest.pieces[0]
    data = bytearray(read_piece(sample_tree, piece))
    data[0] ^= 1
    with pytest.raises(PieceVerificationError):
        verify_piece_bytes(bytes(data), piece)

