"""High-level client session helpers."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class BitswarmSession:
    """Small holder for peer and tracker defaults."""

    peer_id: str
    tracker_url: str | None = None
    token: str | None = None
    peer_urls: list[str] = field(default_factory=list)

