"""Verified downloader."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from bitswarm.protocol.errors import PieceUnavailableError, PieceVerificationError
from bitswarm.protocol.paths import resolve_target_without_symlink_ancestors
from bitswarm.protocol.pieces import create_empty_file_no_symlinks, make_directory_no_symlinks, write_piece
from bitswarm.protocol.schemas import (
    BitswarmManifest,
    BitswarmPiece,
    validate_peer_base_url,
    validate_peer_base_url_with_dns,
)
from bitswarm.protocol.verifier import verify_piece_bytes

from .cache import clear_staging, ensure_staging_guard, prepare_staging, promote_verified_tree
from .transport import PinnedDNSAsyncHTTPTransport

ProgressCallback = Callable[[int, int, str], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class PeerSource:
    """A normalized peer origin plus optional advertised piece ownership."""

    base_url: str
    piece_ids: frozenset[str] | None = None
    pin_host: str | None = None
    pinned_ips: frozenset[str] = field(default_factory=frozenset)


PeerInput = str | PeerSource


def direct_peer_source(base_url: str) -> PeerSource:
    """Create an explicit direct peer source.

    Direct peers are caller-trusted origins. They may be local/private for
    development, but still must be credential-free origin URLs.
    """
    return PeerSource(base_url=validate_peer_base_url(base_url, allow_private=True), piece_ids=None)


def tracker_peer_source(base_url: str, piece_ids: set[str] | frozenset[str]) -> PeerSource:
    """Create a tracker-discovered peer source with strict public-origin validation."""
    checked_base_url, host, pinned_ips = validate_peer_base_url_with_dns(base_url, allow_private=False)
    return PeerSource(
        base_url=checked_base_url,
        piece_ids=frozenset(piece_ids),
        pin_host=host,
        pinned_ips=pinned_ips,
    )


async def download_manifest(
    manifest: BitswarmManifest,
    *,
    peer_urls: list[PeerInput],
    output_path: Path,
    client: httpx.AsyncClient | None = None,
    progress_cb: ProgressCallback | None = None,
) -> Path:
    """Download and verify all manifest pieces from peers."""
    if not peer_urls:
        raise ValueError("at least one peer URL is required")
    peer_sources = _normalize_peer_sources(peer_urls)
    destination = resolve_target_without_symlink_ancestors(output_path)
    single_file_output = manifest.root_kind == "file"
    guard = prepare_staging(destination, manifest)
    staging = guard.path
    owns_client = client is None
    http_client = client or httpx.AsyncClient(
        timeout=30.0,
        transport=PinnedDNSAsyncHTTPTransport(_peer_source_pins(peer_sources)),
    )
    success = False
    try:
        for file in manifest.files:
            if file.size == 0:
                ensure_staging_guard(guard)
                create_empty_file_no_symlinks(staging, file.path, single_file=single_file_output)
        if not single_file_output:
            for directory in manifest.directories:
                ensure_staging_guard(guard)
                make_directory_no_symlinks(staging, directory.path)
        total = len(manifest.pieces)
        for index, piece in enumerate(manifest.pieces, start=1):
            data = await _download_verified_piece(
                http_client,
                peer_sources=peer_sources,
                manifest_id=manifest.manifest_id,
                piece=piece,
            )
            ensure_staging_guard(guard)
            write_piece(staging, piece, data, single_file=single_file_output)
            ensure_staging_guard(guard)
            if progress_cb is not None:
                maybe_awaitable = progress_cb(index, total, piece.piece_id)
                if maybe_awaitable is not None:
                    await maybe_awaitable
        promote_verified_tree(staging, destination, manifest, guard=guard)
        success = True
        return destination
    finally:
        if not success:
            clear_staging(staging)
        if owns_client:
            await http_client.aclose()


async def _download_verified_piece(
    client: httpx.AsyncClient,
    *,
    peer_sources: list[PeerSource],
    manifest_id: str,
    piece: BitswarmPiece,
) -> bytes:
    failures: list[str] = []
    saw_integrity_failure = False
    candidates = [
        peer_source
        for peer_source in peer_sources
        if peer_source.piece_ids is None or piece.piece_id in peer_source.piece_ids
    ]
    if not candidates:
        raise PieceUnavailableError(f"piece {piece.piece_id} was not advertised by any peer")
    for peer_source in candidates:
        url = f"{peer_source.base_url}/api/manifests/{manifest_id}/pieces/{piece.piece_id}"
        try:
            data = await _get_piece_with_size_cap(client, url=url, piece=piece)
            verify_piece_bytes(data, piece)
            return data
        except PieceVerificationError as exc:
            saw_integrity_failure = True
            failures.append(f"{peer_source.base_url}: {exc}")
            continue
        except Exception as exc:
            failures.append(f"{peer_source.base_url}: {exc}")
            continue
    joined = "; ".join(failures)
    if saw_integrity_failure:
        raise PieceVerificationError(f"piece {piece.piece_id} failed verification from all peers: {joined}")
    raise PieceUnavailableError(f"piece {piece.piece_id} unavailable from all peers: {joined}")


def _normalize_peer_sources(peer_urls: list[PeerInput]) -> list[PeerSource]:
    sources: list[PeerSource] = []
    for peer_url in peer_urls:
        if isinstance(peer_url, PeerSource):
            allow_private = peer_url.piece_ids is None
            if allow_private:
                base_url = validate_peer_base_url(peer_url.base_url, allow_private=True)
                sources.append(PeerSource(base_url=base_url, piece_ids=peer_url.piece_ids))
                continue
            base_url, host, pinned_ips = validate_peer_base_url_with_dns(
                peer_url.base_url,
                allow_private=False,
            )
            sources.append(
                PeerSource(
                    base_url=base_url,
                    piece_ids=peer_url.piece_ids,
                    pin_host=host,
                    pinned_ips=pinned_ips or peer_url.pinned_ips,
                )
            )
        else:
            sources.append(direct_peer_source(peer_url))
    return sources


def _peer_source_pins(peer_sources: list[PeerSource]) -> dict[str, tuple[str, ...]]:
    pins: dict[str, tuple[str, ...]] = {}
    for peer_source in peer_sources:
        if peer_source.pin_host is not None:
            current = pins.get(peer_source.pin_host)
            candidate = tuple(peer_source.pinned_ips)
            if current and not candidate:
                continue
            pins[peer_source.pin_host] = candidate
    return pins


async def _get_piece_with_size_cap(
    client: httpx.AsyncClient,
    *,
    url: str,
    piece: BitswarmPiece,
) -> bytes:
    chunks: list[bytes] = []
    observed = 0
    async with client.stream("GET", url) as response:
        response.raise_for_status()
        async for chunk in response.aiter_bytes():
            observed += len(chunk)
            if observed > piece.size:
                raise PieceVerificationError(f"piece {piece.piece_id} exceeded declared size")
            chunks.append(chunk)
    return b"".join(chunks)
