"""Peer helper functions."""

from __future__ import annotations

from time import time

from .schemas import BitswarmAnnounce, BitswarmManifest, BitswarmPeer


def now_ms() -> int:
    return int(time() * 1000)


def peer_from_announce(announce: BitswarmAnnounce) -> BitswarmPeer:
    return BitswarmPeer(
        peer_id=announce.peer_id,
        base_url=announce.base_url,
        manifests=[announce.manifest_id],
        updated_at_ms=now_ms(),
    )


def full_piece_map(manifest: BitswarmManifest) -> list[str]:
    return [piece.piece_id for piece in manifest.pieces]

