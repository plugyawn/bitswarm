"""FastAPI tracker application."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Path

from bitswarm.constants import CONTROL_ID_PATTERN, MAX_ID_LENGTH
from bitswarm.protocol.peer import now_ms
from bitswarm.protocol.schemas import BitswarmAnnounce, BitswarmPeer

from .auth import auth_header, peer_secret_header, validate_bearer_token
from .schemas import TrackerPeersResponse, TrackerPieceMapResponse
from .store import TrackerStore

PathControlId = Annotated[
    str,
    Path(min_length=1, max_length=MAX_ID_LENGTH, pattern=CONTROL_ID_PATTERN),
]


def create_tracker_app(
    *,
    token: str,
    store: TrackerStore | None = None,
    peer_ttl_ms: int = 300_000,
) -> FastAPI:
    if not token:
        raise ValueError("tracker token is required")
    if peer_ttl_ms <= 0:
        raise ValueError("peer_ttl_ms must be positive")
    tracker_store = store or TrackerStore(peer_ttl_ms=peer_ttl_ms)
    app = FastAPI(title="bitswarm-tracker", version="1.0.0a1")
    app.state.bitswarm_tracker_store = tracker_store
    app.state.bitswarm_peer_ttl_ms = peer_ttl_ms

    @app.get("/api/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/api/announces", response_model=BitswarmPeer)
    async def announce(
        announcement: BitswarmAnnounce,
        authorization: str | None = Depends(auth_header),
        peer_secret: str | None = Depends(peer_secret_header),
    ) -> BitswarmPeer:
        validate_bearer_token(authorization, expected_token=token)
        if not peer_secret:
            raise HTTPException(status_code=401, detail="missing peer secret")
        try:
            return tracker_store.announce(announcement, peer_secret=peer_secret, at_ms=now_ms())
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/api/manifests/{manifest_id}/peers", response_model=TrackerPeersResponse)
    async def peers(
        manifest_id: PathControlId,
        authorization: str | None = Depends(auth_header),
    ) -> TrackerPeersResponse:
        validate_bearer_token(authorization, expected_token=token)
        return TrackerPeersResponse(
            manifest_id=manifest_id,
            peers=tracker_store.peers_for_manifest(manifest_id, at_ms=now_ms()),
        )

    @app.get("/api/manifests/{manifest_id}/peers/{peer_id}/pieces", response_model=TrackerPieceMapResponse)
    async def pieces(
        manifest_id: PathControlId,
        peer_id: PathControlId,
        authorization: str | None = Depends(auth_header),
    ) -> TrackerPieceMapResponse:
        validate_bearer_token(authorization, expected_token=token)
        return TrackerPieceMapResponse(
            manifest_id=manifest_id,
            peer_id=peer_id,
            piece_ids=tracker_store.pieces_for_peer(manifest_id=manifest_id, peer_id=peer_id, at_ms=now_ms()),
        )

    return app
