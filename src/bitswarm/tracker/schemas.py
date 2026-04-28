"""Tracker response schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from bitswarm.constants import MAX_TRACKER_PIECES_PER_ANNOUNCE
from bitswarm.protocol.schemas import BitswarmPeer, ControlId


class TrackerPeersResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    manifest_id: ControlId
    peers: list[BitswarmPeer]


class TrackerPieceMapResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    manifest_id: ControlId
    peer_id: ControlId
    piece_ids: list[ControlId] = Field(max_length=MAX_TRACKER_PIECES_PER_ANNOUNCE)
