"""Local Web UI state manager."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any

import httpx

from bitswarm.client.downloader import PeerInput, download_manifest
from bitswarm.client.seeder import _assert_root_identity, _root_identity
from bitswarm.protocol.manifest import create_manifest, load_manifest
from bitswarm.protocol.peer import full_piece_map
from bitswarm.protocol.pieces import piece_by_id, read_piece
from bitswarm.protocol.schemas import BitswarmAnnounce, BitswarmManifest, BitswarmPieceMap
from bitswarm.protocol.verifier import verify_manifest_tree, verify_piece_bytes

from .models import (
    WebUIAnnounceCreate,
    WebUIDownloadCreate,
    WebUIFileView,
    WebUIMessage,
    WebUIPieceView,
    WebUISeedCreate,
    WebUISeedView,
    WebUIStateView,
    WebUITransferView,
)

DownloadFn = Callable[..., Awaitable[Path]]


@dataclass(slots=True)
class TransferRecord:
    transfer_id: str
    manifest: BitswarmManifest
    output_path: Path
    peer_inputs: list[PeerInput]
    peer_count: int
    status: str = "queued"
    completed_pieces: int = 0
    completed_bytes: int = 0
    down_bps: float = 0.0
    active_piece_id: str | None = None
    error: str | None = None
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    started_monotonic: float | None = None
    task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class SeedRecord:
    seed_id: str
    root_path: Path
    manifest: BitswarmManifest
    root_identity: tuple[int, ...]
    status: str = "seeding"
    error: str | None = None
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class WebUIState:
    """Holds local UI transfer state and mounted seed roots."""

    def __init__(self, *, download_fn: DownloadFn = download_manifest) -> None:
        self._download_fn = download_fn
        self._lock = RLock()
        self._transfers: dict[str, TransferRecord] = {}
        self._seeds: dict[str, SeedRecord] = {}
        self._seeds_by_manifest: dict[str, SeedRecord] = {}

    def snapshot(self) -> WebUIStateView:
        with self._lock:
            return WebUIStateView(
                transfers=[self._transfer_view(record) for record in self._transfers.values()],
                seeds=[self._seed_view(record) for record in self._seeds.values()],
            )

    def transfer(self, transfer_id: str) -> WebUITransferView:
        with self._lock:
            record = self._transfers.get(transfer_id)
            if record is None:
                raise KeyError(transfer_id)
            return self._transfer_view(record)

    async def add_download(self, request: WebUIDownloadCreate) -> WebUITransferView:
        manifest = load_manifest(Path(request.manifest_path))
        peer_inputs = await self._peer_inputs(
            request=request,
            expected_piece_ids={piece.piece_id for piece in manifest.pieces},
        )
        if not peer_inputs:
            raise ValueError("download requires at least one direct peer or tracker-discovered peer")
        record = TransferRecord(
            transfer_id=_new_id("dl"),
            manifest=manifest,
            output_path=Path(request.output_path),
            peer_inputs=peer_inputs,
            peer_count=len(peer_inputs),
        )
        with self._lock:
            self._transfers[record.transfer_id] = record
        if request.auto_start:
            self.start_download(record.transfer_id)
        return self.transfer(record.transfer_id)

    def start_download(self, transfer_id: str) -> WebUITransferView:
        with self._lock:
            record = self._transfers.get(transfer_id)
            if record is None:
                raise KeyError(transfer_id)
            if record.status == "completed":
                return self._transfer_view(record)
            if record.task is not None and not record.task.done():
                return self._transfer_view(record)
            record.status = "downloading"
            record.error = None
            record.started_monotonic = time.monotonic()
            record.updated_at_ms = _now_ms()
            record.task = asyncio.create_task(self._run_download(record.transfer_id))
            return self._transfer_view(record)

    async def cancel_download(self, transfer_id: str) -> WebUITransferView:
        with self._lock:
            record = self._transfers.get(transfer_id)
            if record is None:
                raise KeyError(transfer_id)
            task = record.task
            record.status = "cancelled"
            record.updated_at_ms = _now_ms()
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        return self.transfer(transfer_id)

    async def add_seed(self, request: WebUISeedCreate) -> WebUISeedView:
        root_path = Path(request.root_path)
        if request.manifest_path is not None:
            manifest = load_manifest(Path(request.manifest_path))
        else:
            kwargs: dict[str, Any] = {}
            if request.piece_size is not None:
                kwargs["piece_size"] = request.piece_size
            if request.name is not None:
                kwargs["name"] = request.name
            manifest = create_manifest(root_path, **kwargs)
        verify_manifest_tree(root_path, manifest)
        root_identity = _root_identity(root_path, manifest)
        record = SeedRecord(
            seed_id=_new_id("seed"),
            root_path=root_path,
            manifest=manifest,
            root_identity=root_identity,
        )
        with self._lock:
            if manifest.manifest_id in self._seeds_by_manifest:
                raise ValueError(f"manifest is already seeded: {manifest.manifest_id}")
            self._seeds[record.seed_id] = record
            self._seeds_by_manifest[manifest.manifest_id] = record
            return self._seed_view(record)

    async def remove_seed(self, seed_id: str) -> WebUIMessage:
        with self._lock:
            record = self._seeds.pop(seed_id, None)
            if record is None:
                raise KeyError(seed_id)
            self._seeds_by_manifest.pop(record.manifest.manifest_id, None)
        return WebUIMessage(detail=f"removed {seed_id}")

    async def announce_seed(self, seed_id: str, request: WebUIAnnounceCreate) -> WebUIMessage:
        with self._lock:
            record = self._seeds.get(seed_id)
            if record is None:
                raise KeyError(seed_id)
            announcement = BitswarmAnnounce(
                peer_id=request.peer_id,
                base_url=request.base_url,
                manifest_id=record.manifest.manifest_id,
                piece_ids=full_piece_map(record.manifest),
            )
        async with httpx.AsyncClient(base_url=request.tracker_url, timeout=10.0) as client:
            response = await client.post(
                "/api/announces",
                headers={
                    "Authorization": f"Bearer {request.token}",
                    "X-Bitswarm-Peer-Secret": request.peer_secret,
                },
                json={
                    **announcement.model_dump(mode="json"),
                    "base_url": str(announcement.base_url).rstrip("/"),
                },
            )
            response.raise_for_status()
        return WebUIMessage(detail=f"announced {request.peer_id}")

    def seeded_manifest(self, manifest_id: str) -> BitswarmManifest:
        with self._lock:
            record = self._seeds_by_manifest.get(manifest_id)
            if record is None:
                raise KeyError(manifest_id)
            return record.manifest

    def seeded_piece_map(self, manifest_id: str) -> BitswarmPieceMap:
        manifest = self.seeded_manifest(manifest_id)
        return BitswarmPieceMap(manifest_id=manifest.manifest_id, piece_ids=full_piece_map(manifest))

    def seeded_piece(self, manifest_id: str, piece_id: str) -> bytes:
        with self._lock:
            record = self._seeds_by_manifest.get(manifest_id)
            if record is None:
                raise KeyError(manifest_id)
            piece = piece_by_id(record.manifest, piece_id)
            try:
                verify_manifest_tree(record.root_path, record.manifest)
                _assert_root_identity(record.root_path, record.manifest, record.root_identity)
                data = read_piece(record.root_path, piece)
                verify_piece_bytes(data, piece)
                _assert_root_identity(record.root_path, record.manifest, record.root_identity)
                return data
            except Exception as exc:
                record.status = "failed"
                record.error = str(exc)
                record.updated_at_ms = _now_ms()
                raise

    async def _peer_inputs(
        self,
        *,
        request: WebUIDownloadCreate,
        expected_piece_ids: set[str],
    ) -> list[PeerInput]:
        peers: list[PeerInput] = list(request.peers)
        if request.tracker_url:
            if not request.token:
                raise ValueError("token is required when tracker_url is set")
            from bitswarm.cli import _tracker_peers

            peers.extend(
                _tracker_peers(
                    tracker_url=request.tracker_url,
                    manifest_id=load_manifest(Path(request.manifest_path)).manifest_id,
                    token=request.token,
                    expected_piece_ids=expected_piece_ids,
                )
            )
        return peers

    async def _run_download(self, transfer_id: str) -> None:
        try:
            with self._lock:
                record = self._transfers[transfer_id]
                manifest = record.manifest
                output_path = record.output_path
                peer_inputs = list(record.peer_inputs)

            async def progress(done: int, total: int, piece_id: str) -> None:
                self._mark_progress(transfer_id, done=done, total=total, piece_id=piece_id)

            await self._download_fn(
                manifest,
                peer_urls=peer_inputs,
                output_path=output_path,
                progress_cb=progress,
            )
            with self._lock:
                record = self._transfers[transfer_id]
                record.status = "completed"
                record.completed_pieces = len(record.manifest.pieces)
                record.completed_bytes = record.manifest.total_size
                record.active_piece_id = None
                record.down_bps = _speed(record)
                record.updated_at_ms = _now_ms()
        except asyncio.CancelledError:
            with self._lock:
                record = self._transfers.get(transfer_id)
                if record is not None:
                    record.status = "cancelled"
                    record.active_piece_id = None
                    record.updated_at_ms = _now_ms()
            raise
        except Exception as exc:
            with self._lock:
                record = self._transfers.get(transfer_id)
                if record is not None:
                    record.status = "failed"
                    record.error = str(exc)
                    record.active_piece_id = None
                    record.down_bps = _speed(record)
                    record.updated_at_ms = _now_ms()

    def _mark_progress(self, transfer_id: str, *, done: int, total: int, piece_id: str) -> None:
        with self._lock:
            record = self._transfers.get(transfer_id)
            if record is None:
                return
            record.completed_pieces = done
            record.completed_bytes = sum(piece.size for piece in record.manifest.pieces[:done])
            record.active_piece_id = None if done >= total else piece_id
            record.down_bps = _speed(record)
            record.updated_at_ms = _now_ms()

    def _transfer_view(self, record: TransferRecord) -> WebUITransferView:
        total_pieces = len(record.manifest.pieces)
        progress = (
            1.0
            if record.manifest.total_size == 0
            else record.completed_bytes / record.manifest.total_size
        )
        done_piece_ids = {piece.piece_id for piece in record.manifest.pieces[: record.completed_pieces]}
        files = _file_views(record.manifest, completed_bytes=record.completed_bytes)
        pieces = [
            WebUIPieceView(
                piece_id=piece.piece_id,
                size=piece.size,
                status=(
                    "done"
                    if piece.piece_id in done_piece_ids
                    else "active"
                    if piece.piece_id == record.active_piece_id
                    else "pending"
                ),
            )
            for piece in record.manifest.pieces
        ]
        return WebUITransferView(
            transfer_id=record.transfer_id,
            manifest_id=record.manifest.manifest_id,
            name=record.manifest.name,
            status=record.status,  # type: ignore[arg-type]
            output_path=str(record.output_path),
            total_bytes=record.manifest.total_size,
            completed_bytes=min(record.completed_bytes, record.manifest.total_size),
            total_pieces=total_pieces,
            completed_pieces=min(record.completed_pieces, total_pieces),
            progress=max(0.0, min(progress, 1.0)),
            down_bps=max(record.down_bps, 0.0),
            peer_count=record.peer_count,
            active_piece_id=record.active_piece_id,
            error=record.error,
            created_at_ms=record.created_at_ms,
            updated_at_ms=record.updated_at_ms,
            files=files,
            pieces=pieces,
        )

    def _seed_view(self, record: SeedRecord) -> WebUISeedView:
        return WebUISeedView(
            seed_id=record.seed_id,
            manifest_id=record.manifest.manifest_id,
            name=record.manifest.name,
            root_path=str(record.root_path),
            status=record.status,  # type: ignore[arg-type]
            total_bytes=record.manifest.total_size,
            total_pieces=len(record.manifest.pieces),
            created_at_ms=record.created_at_ms,
            updated_at_ms=record.updated_at_ms,
            error=record.error,
        )


def _file_views(manifest: BitswarmManifest, *, completed_bytes: int) -> list[WebUIFileView]:
    remaining = completed_bytes
    views: list[WebUIFileView] = []
    for file in manifest.files:
        done = min(file.size, max(remaining, 0))
        remaining -= done
        progress = 1.0 if file.size == 0 else done / file.size
        views.append(WebUIFileView(path=file.path, size=file.size, progress=max(0.0, min(progress, 1.0))))
    return views


def _speed(record: TransferRecord) -> float:
    if record.started_monotonic is None:
        return 0.0
    elapsed = max(time.monotonic() - record.started_monotonic, 0.001)
    return record.completed_bytes / elapsed


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_ms() -> int:
    return int(time.time() * 1000)
