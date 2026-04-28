"""Web UI API models.

These schemas are intentionally outside the public transfer protocol. They
describe local operator state for a browser UI and do not add any wire authority
to Bitswarm's peer/tracker protocol.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr

from bitswarm.protocol.schemas import ControlId, StrictModel

TransferStatus = Literal["queued", "downloading", "completed", "failed", "cancelled"]
SeedStatus = Literal["seeding", "failed"]


class WebUIDownloadCreate(StrictModel):
    manifest_path: StrictStr = Field(min_length=1)
    output_path: StrictStr = Field(min_length=1)
    peers: list[StrictStr] = Field(default_factory=list)
    tracker_url: StrictStr | None = None
    token: StrictStr | None = None
    auto_start: StrictBool = True


class WebUISeedCreate(StrictModel):
    root_path: StrictStr = Field(min_length=1)
    manifest_path: StrictStr | None = None
    piece_size: StrictInt | None = Field(default=None, gt=0)
    name: StrictStr | None = None


class WebUIAnnounceCreate(StrictModel):
    tracker_url: StrictStr = Field(min_length=1)
    token: StrictStr = Field(min_length=1)
    peer_secret: StrictStr = Field(min_length=1)
    peer_id: StrictStr = Field(min_length=1)
    base_url: StrictStr = Field(min_length=1)


class WebUIFileView(StrictModel):
    path: StrictStr
    size: StrictInt = Field(ge=0)
    progress: StrictFloat = Field(ge=0.0, le=1.0)


class WebUIPieceView(StrictModel):
    piece_id: ControlId
    size: StrictInt = Field(ge=0)
    status: Literal["pending", "active", "done"]


class WebUITransferView(StrictModel):
    transfer_id: ControlId
    manifest_id: ControlId
    name: StrictStr
    status: TransferStatus
    output_path: StrictStr
    total_bytes: StrictInt = Field(ge=0)
    completed_bytes: StrictInt = Field(ge=0)
    total_pieces: StrictInt = Field(ge=0)
    completed_pieces: StrictInt = Field(ge=0)
    progress: StrictFloat = Field(ge=0.0, le=1.0)
    down_bps: StrictFloat = Field(ge=0.0)
    peer_count: StrictInt = Field(ge=0)
    active_piece_id: ControlId | None = None
    error: StrictStr | None = None
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    files: list[WebUIFileView]
    pieces: list[WebUIPieceView]


class WebUISeedView(StrictModel):
    seed_id: ControlId
    manifest_id: ControlId
    name: StrictStr
    root_path: StrictStr
    status: SeedStatus
    total_bytes: StrictInt = Field(ge=0)
    total_pieces: StrictInt = Field(ge=0)
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    error: StrictStr | None = None


class WebUIStateView(StrictModel):
    transfers: list[WebUITransferView]
    seeds: list[WebUISeedView]


class WebUIMessage(StrictModel):
    ok: StrictBool = True
    detail: StrictStr
