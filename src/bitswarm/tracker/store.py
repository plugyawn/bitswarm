"""In-memory tracker store."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock

from pydantic import ValidationError

from bitswarm.constants import MAX_TRACKER_MANIFESTS_PER_PEER
from bitswarm.protocol.schemas import BitswarmAnnounce, BitswarmPeer, validate_peer_base_url


@dataclass(slots=True)
class ManifestAvailability:
    piece_ids: set[str]
    base_url: str
    updated_at_ms: int


@dataclass(slots=True)
class PeerState:
    peer: BitswarmPeer
    peer_secret: str
    manifests: dict[str, ManifestAvailability] = field(default_factory=dict)


class TrackerStore:
    def __init__(self, *, peer_ttl_ms: int = 300_000) -> None:
        if peer_ttl_ms <= 0:
            raise ValueError("peer_ttl_ms must be positive")
        self._peers: dict[str, PeerState] = {}
        self._peer_secrets: dict[str, str] = {}
        self._peer_ttl_ms = peer_ttl_ms
        self._lock = Lock()

    def announce(self, announce: BitswarmAnnounce, *, peer_secret: str, at_ms: int) -> BitswarmPeer:
        with self._lock:
            self._expire_stale_locked(at_ms=at_ms)
            known_secret = self._peer_secrets.get(announce.peer_id)
            if known_secret is not None and known_secret != peer_secret:
                raise PermissionError("peer_id is already bound to a different peer secret")
            state = self._peers.get(announce.peer_id)
            if state is not None and state.peer_secret != peer_secret:
                raise PermissionError("peer_id is already bound to a different peer secret")
            if state is None:
                state = PeerState(
                    peer=BitswarmPeer(
                        peer_id=announce.peer_id,
                        base_url=announce.base_url,
                        manifests=[],
                        updated_at_ms=at_ms,
                    ),
                    peer_secret=peer_secret,
                )
                self._peers[announce.peer_id] = state
            if (
                announce.manifest_id not in state.manifests
                and len(state.manifests) >= MAX_TRACKER_MANIFESTS_PER_PEER
            ):
                raise ValueError("peer has too many advertised manifests")
            if state is not None:
                state.manifests[announce.manifest_id] = ManifestAvailability(
                    piece_ids=set(announce.piece_ids),
                    base_url=str(announce.base_url),
                    updated_at_ms=at_ms,
                )
            manifests = sorted(state.manifests)
            peer = BitswarmPeer(
                peer_id=announce.peer_id,
                base_url=announce.base_url,
                manifests=manifests,
                updated_at_ms=at_ms,
            )
            state.peer = peer
            self._peer_secrets[announce.peer_id] = peer_secret
            return peer

    def peers_for_manifest(self, manifest_id: str, *, at_ms: int) -> list[BitswarmPeer]:
        with self._lock:
            self._expire_stale_locked(at_ms=at_ms)
            peers: list[BitswarmPeer] = []
            for state in self._peers.values():
                availability = state.manifests.get(manifest_id)
                if availability is None:
                    continue
                try:
                    base_url = validate_peer_base_url(availability.base_url, allow_private=False)
                    peers.append(
                        BitswarmPeer(
                            peer_id=state.peer.peer_id,
                            base_url=base_url,
                            manifests=[manifest_id],
                            updated_at_ms=availability.updated_at_ms,
                        )
                    )
                except (ValueError, ValidationError):
                    continue
            return peers

    def pieces_for_peer(self, *, manifest_id: str, peer_id: str, at_ms: int) -> list[str]:
        with self._lock:
            self._expire_stale_locked(at_ms=at_ms)
            state = self._peers.get(peer_id)
            if state is None:
                return []
            availability = state.manifests.get(manifest_id)
            if availability is None:
                return []
            return sorted(availability.piece_ids)

    def _expire_stale_locked(self, *, at_ms: int) -> None:
        for peer_id, state in list(self._peers.items()):
            for manifest_id, availability in list(state.manifests.items()):
                if at_ms - availability.updated_at_ms >= self._peer_ttl_ms:
                    state.manifests.pop(manifest_id, None)
            if not state.manifests:
                self._peers.pop(peer_id, None)
                continue
            state.peer = state.peer.model_copy(
                update={
                    "manifests": sorted(state.manifests),
                    "updated_at_ms": max(item.updated_at_ms for item in state.manifests.values()),
                }
            )
