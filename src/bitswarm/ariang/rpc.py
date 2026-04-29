"""aria2 JSON-RPC compatibility bridge backed by Bitswarm downloads.

This module intentionally implements a local operator API, not public Bitswarm
wire protocol. The public peer/tracker protocol remains in ``bitswarm.protocol``
and ``bitswarm.tracker``.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import httpx

from bitswarm.client.downloader import PeerInput, ProgressCallback, download_manifest
from bitswarm.protocol.manifest import load_manifest
from bitswarm.protocol.paths import resolve_target_without_symlink_ancestors
from bitswarm.protocol.schemas import BitswarmManifest

from .runs import RunRecord, RunRegistry
from .telemetry import TelemetryProgress, TelemetryProvider, WorkloadTelemetry

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
DownloadFn = Callable[
    [BitswarmManifest, list[PeerInput], Path, ProgressCallback | None],
    Awaitable[Path],
]

_SUPPORTED_METHODS = {
    "aria2.addUri",
    "aria2.changeGlobalOption",
    "aria2.changeOption",
    "aria2.changePosition",
    "aria2.forcePause",
    "aria2.forcePauseAll",
    "aria2.forceRemove",
    "aria2.getFiles",
    "aria2.getGlobalOption",
    "aria2.getGlobalStat",
    "aria2.getOption",
    "aria2.getPeers",
    "aria2.getServers",
    "aria2.getSessionInfo",
    "aria2.getUris",
    "aria2.getVersion",
    "aria2.pause",
    "aria2.pauseAll",
    "aria2.purgeDownloadResult",
    "aria2.remove",
    "aria2.removeDownloadResult",
    "aria2.tellActive",
    "aria2.tellStatus",
    "aria2.tellStopped",
    "aria2.tellWaiting",
    "aria2.unpause",
    "aria2.unpauseAll",
    "system.listMethods",
    "system.listNotifications",
    "system.multicall",
}


@dataclass(slots=True)
class ParsedBitswarmUri:
    manifest_ref: str
    peer_urls: list[str]
    output_path: Path | None
    tracker_url: str | None = None
    tracker_token: str | None = None


@dataclass(slots=True)
class Transfer:
    gid: str
    uri: str
    manifest: BitswarmManifest
    peer_urls: list[str]
    output_path: Path
    options: dict[str, JsonValue]
    status: str = "waiting"
    completed_pieces: int = 0
    completed_length: int = 0
    download_speed: int = 0
    error_code: str = "0"
    error_message: str = ""
    created_at: float = field(default_factory=time.monotonic)
    updated_at: float = field(default_factory=time.monotonic)
    stopped_at: float = 0.0
    task: asyncio.Task[None] | None = None
    _last_sample_time: float = field(default_factory=time.monotonic)
    _last_sample_bytes: int = 0

    @property
    def total_length(self) -> int:
        return self.manifest.total_size

    @property
    def piece_length(self) -> int:
        return self.manifest.piece_size

    @property
    def num_pieces(self) -> int:
        return len(self.manifest.pieces)

    def refresh_completed_length(self) -> None:
        self.completed_length = sum(piece.size for piece in self.manifest.pieces[: self.completed_pieces])

    def update_progress(self, done: int) -> None:
        self.completed_pieces = max(0, min(done, self.num_pieces))
        self.refresh_completed_length()
        now = time.monotonic()
        elapsed = max(now - self._last_sample_time, 1e-6)
        delta = max(self.completed_length - self._last_sample_bytes, 0)
        self.download_speed = int(delta / elapsed)
        self._last_sample_time = now
        self._last_sample_bytes = self.completed_length
        self.updated_at = now


class AriaNgBridge:
    """Local aria2-compatible facade for AriaNg."""

    def __init__(
        self,
        *,
        download_fn: DownloadFn | None = None,
        default_output_dir: Path | None = None,
        telemetry_provider: TelemetryProvider | None = None,
        run_registry: RunRegistry | None = None,
    ) -> None:
        self._download_fn = download_fn or _default_download
        self._default_output_dir = default_output_dir or Path.cwd() / "bitswarm-downloads"
        self._telemetry_provider = telemetry_provider
        self._run_registry = run_registry
        self._transfers: dict[str, Transfer] = {}
        self._lock = asyncio.Lock()
        self._global_options: dict[str, JsonValue] = {
            "dir": str(self._default_output_dir),
            "max-concurrent-downloads": "5",
            "continue": "true",
        }
        self._session_id = f"bitswarm-{secrets.token_hex(8)}"

    async def handle_jsonrpc(self, payload: dict[str, Any]) -> dict[str, JsonValue]:
        request_id = _coerce_json_value(payload.get("id"))
        try:
            if payload.get("jsonrpc") != "2.0":
                raise RpcFailure(-32600, "invalid JSON-RPC version")
            method = payload.get("method")
            if not isinstance(method, str):
                raise RpcFailure(-32600, "method must be a string")
            params = payload.get("params", [])
            if params is None:
                params = []
            if not isinstance(params, list):
                raise RpcFailure(-32602, "params must be an array")
            result = await self._dispatch(method, _strip_token(params))
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except RpcFailure as exc:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": exc.code, "message": exc.message},
            }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    async def _dispatch(self, method: str, params: list[Any]) -> JsonValue:
        if method not in _SUPPORTED_METHODS:
            raise RpcFailure(-32601, f"unsupported method: {method}")
        if method == "system.listMethods":
            return sorted(_SUPPORTED_METHODS)
        if method == "system.listNotifications":
            return []
        if method == "system.multicall":
            return await self._multicall(params)
        method_name = method.removeprefix("aria2.")
        handler = getattr(self, f"_rpc_{method_name}", None)
        if handler is None:
            raise RpcFailure(-32601, f"unsupported method: {method}")
        return await handler(params)

    async def _multicall(self, params: list[Any]) -> JsonValue:
        if len(params) != 1 or not isinstance(params[0], list):
            raise RpcFailure(-32602, "system.multicall expects a method array")
        results: list[JsonValue] = []
        for call in params[0]:
            if not isinstance(call, dict):
                raise RpcFailure(-32602, "multicall entries must be objects")
            method = call.get("methodName")
            call_params = call.get("params", [])
            if not isinstance(method, str) or not isinstance(call_params, list):
                raise RpcFailure(-32602, "invalid multicall entry")
            try:
                results.append([await self._dispatch(method, _strip_token(call_params))])
            except RpcFailure as exc:
                results.append({"faultCode": exc.code, "faultString": exc.message})
        return results

    async def _rpc_addUri(self, params: list[Any]) -> JsonValue:
        if not params or not isinstance(params[0], list) or not params[0]:
            raise RpcFailure(-32602, "aria2.addUri expects at least one URI")
        uri = params[0][0]
        if not isinstance(uri, str):
            raise RpcFailure(-32602, "Bitswarm manifest URI must be a string")
        options = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
        parsed = await _parse_bitswarm_uri(uri, options=options)
        manifest = await _load_manifest_ref(parsed.manifest_ref)
        gid = secrets.token_hex(8)
        output_path = parsed.output_path or _output_path_from_options(
            options,
            default_dir=self._default_output_dir,
            manifest=manifest,
        )
        transfer = Transfer(
            gid=gid,
            uri=uri,
            manifest=manifest,
            peer_urls=parsed.peer_urls,
            output_path=resolve_target_without_symlink_ancestors(output_path),
            options={str(key): _coerce_json_value(value) for key, value in options.items()},
        )
        async with self._lock:
            self._transfers[gid] = transfer
        if str(options.get("pause", "")).lower() == "true":
            transfer.status = "paused"
        else:
            self._start_transfer(transfer)
        return gid

    async def _rpc_tellActive(self, params: list[Any]) -> JsonValue:
        return await self._select_tasks(["active"], _fields_from_tail(params))

    async def _rpc_tellWaiting(self, params: list[Any]) -> JsonValue:
        offset = int(params[0]) if params and isinstance(params[0], int) else 0
        num = int(params[1]) if len(params) > 1 and isinstance(params[1], int) else 1000
        fields = params[2] if len(params) > 2 and isinstance(params[2], list) else None
        rows = await self._select_tasks(["waiting", "paused"], fields)
        return rows[offset : offset + num]

    async def _rpc_tellStopped(self, params: list[Any]) -> JsonValue:
        offset = int(params[0]) if params and isinstance(params[0], int) else -1
        num = int(params[1]) if len(params) > 1 and isinstance(params[1], int) else 1000
        fields = params[2] if len(params) > 2 and isinstance(params[2], list) else None
        rows = await self._select_tasks(["complete", "error", "removed"], fields)
        if offset < 0:
            return rows[-num:] if num >= 0 else rows
        return rows[offset : offset + num]

    async def _rpc_tellStatus(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        fields = params[1] if len(params) > 1 and isinstance(params[1], list) else None
        transfer = await self._transfer_by_gid(gid)
        if transfer is not None:
            return self._task_view(transfer, fields=fields)
        task = await self._telemetry_task_by_gid(gid, fields=fields)
        if task is not None:
            return task
        task = await self._run_task_by_gid(gid, fields=fields)
        if task is not None:
            return task
        raise RpcFailure(1, f"gid not found: {gid}")

    async def _rpc_getUris(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid):
            return [{"uri": f"bitswarm-telemetry:{gid}", "status": "used"}]
        if await self._run_gid_exists(gid):
            return [{"uri": f"bitswarm-run:{gid}", "status": "used"}]
        transfer = await self._transfer_from_params(params)
        return [{"uri": transfer.uri, "status": "used"}]

    async def _rpc_getFiles(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        telemetry_files = await self._telemetry_files_by_gid(gid)
        if telemetry_files is not None:
            return telemetry_files
        run_files = await self._run_files_by_gid(gid)
        if run_files is not None:
            return run_files
        transfer = await self._transfer_from_params(params)
        return self._file_views(transfer)

    async def _rpc_getPeers(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        telemetry_peers = await self._telemetry_peers_by_gid(gid)
        if telemetry_peers is not None:
            return telemetry_peers
        run_peers = await self._run_peers_by_gid(gid)
        if run_peers is not None:
            return run_peers
        transfer = await self._transfer_from_params(params)
        bitfield = _bitfield(transfer.completed_pieces, transfer.num_pieces)
        return [
            {
                "peerId": f"bitswarm-peer-{index}",
                "ip": urlparse(peer_url).hostname or peer_url,
                "port": str(urlparse(peer_url).port or ""),
                "bitfield": bitfield,
                "amChoking": "false",
                "peerChoking": "false",
                "downloadSpeed": str(transfer.download_speed),
                "uploadSpeed": "0",
                "seeder": "true" if transfer.status == "complete" else "false",
            }
            for index, peer_url in enumerate(transfer.peer_urls, start=1)
        ]

    async def _rpc_getServers(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid):
            return [{"index": "1", "servers": [{"uri": f"bitswarm-telemetry:{gid}", "downloadSpeed": "0"}]}]
        if await self._run_gid_exists(gid):
            return [{"index": "1", "servers": [{"uri": f"bitswarm-run:{gid}", "downloadSpeed": "0"}]}]
        transfer = await self._transfer_from_params(params)
        return [
            {
                "index": "1",
                "servers": [
                    {
                        "uri": peer_url,
                        "currentUri": peer_url,
                        "downloadSpeed": str(transfer.download_speed),
                    }
                    for peer_url in transfer.peer_urls
                ],
            }
        ]

    async def _rpc_getOption(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return {}
        transfer = await self._transfer_from_params(params)
        return dict(transfer.options)

    async def _rpc_changeOption(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return "OK"
        transfer = await self._transfer_from_params(params)
        options = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
        transfer.options.update({str(key): _coerce_json_value(value) for key, value in options.items()})
        return "OK"

    async def _rpc_getGlobalOption(self, params: list[Any]) -> JsonValue:
        return dict(self._global_options)

    async def _rpc_changeGlobalOption(self, params: list[Any]) -> JsonValue:
        options = params[0] if params and isinstance(params[0], dict) else {}
        self._global_options.update({str(key): _coerce_json_value(value) for key, value in options.items()})
        return "OK"

    async def _rpc_getGlobalStat(self, params: list[Any]) -> JsonValue:
        async with self._lock:
            transfers = list(self._transfers.values())
        active = [transfer for transfer in transfers if transfer.status == "active"]
        waiting = [transfer for transfer in transfers if transfer.status in {"waiting", "paused"}]
        stopped = [transfer for transfer in transfers if transfer.status in {"complete", "error", "removed"}]
        telemetry_tasks = await self._telemetry_task_views(fields=None)
        telemetry_active = [task for task in telemetry_tasks if task.get("status") == "active"]
        telemetry_waiting = [task for task in telemetry_tasks if task.get("status") in {"waiting", "paused"}]
        telemetry_stopped = [
            task for task in telemetry_tasks if task.get("status") in {"complete", "error", "removed"}
        ]
        run_tasks = await self._run_task_views(fields=None)
        run_active = [task for task in run_tasks if task.get("status") == "active"]
        run_waiting = [task for task in run_tasks if task.get("status") in {"waiting", "paused"}]
        run_stopped = [task for task in run_tasks if task.get("status") in {"complete", "error", "removed"}]
        return {
            "downloadSpeed": str(
                sum(transfer.download_speed for transfer in active)
                + sum(int(str(task.get("downloadSpeed") or "0")) for task in telemetry_active)
                + sum(int(str(task.get("downloadSpeed") or "0")) for task in run_active)
            ),
            "uploadSpeed": "0",
            "numActive": str(len(active) + len(telemetry_active) + len(run_active)),
            "numWaiting": str(len(waiting) + len(telemetry_waiting) + len(run_waiting)),
            "numStopped": str(len(stopped) + len(telemetry_stopped) + len(run_stopped)),
            "numStoppedTotal": str(len(stopped) + len(telemetry_stopped) + len(run_stopped)),
        }

    async def _rpc_getVersion(self, params: list[Any]) -> JsonValue:
        return {
            "version": "bitswarm-aria2-bridge/1.0.0a1",
            "enabledFeatures": ["Bitswarm", "HTTPS", "Async DNS"],
        }

    async def _rpc_getSessionInfo(self, params: list[Any]) -> JsonValue:
        return {"sessionId": self._session_id}

    async def _rpc_pause(self, params: list[Any]) -> JsonValue:
        return await self._pause_one(params)

    async def _rpc_forcePause(self, params: list[Any]) -> JsonValue:
        return await self._pause_one(params)

    async def _rpc_pauseAll(self, params: list[Any]) -> JsonValue:
        return await self._pause_all()

    async def _rpc_forcePauseAll(self, params: list[Any]) -> JsonValue:
        return await self._pause_all()

    async def _rpc_unpause(self, params: list[Any]) -> JsonValue:
        transfer = await self._transfer_from_params(params)
        if transfer.status == "paused":
            transfer.status = "waiting"
            self._start_transfer(transfer)
        return transfer.gid

    async def _rpc_unpauseAll(self, params: list[Any]) -> JsonValue:
        async with self._lock:
            transfers = list(self._transfers.values())
        for transfer in transfers:
            if transfer.status == "paused":
                transfer.status = "waiting"
                self._start_transfer(transfer)
        return "OK"

    async def _rpc_remove(self, params: list[Any]) -> JsonValue:
        return await self._remove_one(params)

    async def _rpc_forceRemove(self, params: list[Any]) -> JsonValue:
        return await self._remove_one(params)

    async def _rpc_removeDownloadResult(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return "OK"
        async with self._lock:
            self._transfers.pop(gid, None)
        return "OK"

    async def _rpc_purgeDownloadResult(self, params: list[Any]) -> JsonValue:
        async with self._lock:
            for gid, transfer in list(self._transfers.items()):
                if transfer.status in {"complete", "error", "removed"}:
                    self._transfers.pop(gid, None)
        return "OK"

    async def _rpc_changePosition(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return gid
        transfer = await self._transfer_from_params(params)
        return transfer.gid

    async def _select_tasks(
        self,
        statuses: list[str],
        fields: list[Any] | None,
    ) -> list[dict[str, JsonValue]]:
        async with self._lock:
            selected = [transfer for transfer in self._transfers.values() if transfer.status in statuses]
        rows = [self._task_view(transfer, fields=fields) for transfer in selected]
        rows.extend(await self._telemetry_task_views(statuses=statuses, fields=fields))
        rows.extend(await self._run_task_views(statuses=statuses, fields=fields))
        return rows

    async def _transfer_from_params(self, params: list[Any]) -> Transfer:
        gid = _gid_from_params(params)
        transfer = await self._transfer_by_gid(gid)
        if transfer is None:
            raise RpcFailure(1, f"gid not found: {gid}")
        return transfer

    async def _transfer_by_gid(self, gid: str) -> Transfer | None:
        async with self._lock:
            return self._transfers.get(gid)

    async def _pause_one(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return gid
        transfer = await self._transfer_from_params(params)
        if transfer.task is not None and not transfer.task.done():
            transfer.task.cancel()
        transfer.status = "paused"
        transfer.download_speed = 0
        transfer.updated_at = time.monotonic()
        return transfer.gid

    async def _pause_all(self) -> JsonValue:
        async with self._lock:
            transfers = list(self._transfers.values())
        for transfer in transfers:
            if transfer.status in {"active", "waiting"}:
                if transfer.task is not None and not transfer.task.done():
                    transfer.task.cancel()
                transfer.status = "paused"
                transfer.download_speed = 0
                transfer.updated_at = time.monotonic()
        return "OK"

    async def _remove_one(self, params: list[Any]) -> JsonValue:
        gid = _gid_from_params(params)
        if await self._telemetry_gid_exists(gid) or await self._run_gid_exists(gid):
            return gid
        transfer = await self._transfer_from_params(params)
        if transfer.task is not None and not transfer.task.done():
            transfer.task.cancel()
        transfer.status = "removed"
        transfer.download_speed = 0
        transfer.stopped_at = time.monotonic()
        transfer.updated_at = transfer.stopped_at
        return transfer.gid

    def _start_transfer(self, transfer: Transfer) -> None:
        if transfer.task is not None and not transfer.task.done():
            return
        transfer.status = "active"
        transfer.download_speed = 0
        transfer.completed_pieces = 0
        transfer.completed_length = 0
        transfer._last_sample_time = time.monotonic()
        transfer._last_sample_bytes = 0
        transfer.task = asyncio.create_task(self._run_transfer(transfer))

    async def _run_transfer(self, transfer: Transfer) -> None:
        async def progress(done: int, total: int, piece_id: str) -> None:
            del total, piece_id
            transfer.update_progress(done)

        try:
            await self._download_fn(transfer.manifest, transfer.peer_urls, transfer.output_path, progress)
            transfer.completed_pieces = transfer.num_pieces
            transfer.completed_length = transfer.total_length
            transfer.download_speed = 0
            transfer.status = "complete"
            transfer.stopped_at = time.monotonic()
            transfer.updated_at = transfer.stopped_at
        except asyncio.CancelledError:
            transfer.download_speed = 0
            transfer.updated_at = time.monotonic()
        except Exception as exc:
            transfer.status = "error"
            transfer.error_code = "1"
            transfer.error_message = str(exc)
            transfer.download_speed = 0
            transfer.stopped_at = time.monotonic()
            transfer.updated_at = transfer.stopped_at

    def _task_view(self, transfer: Transfer, *, fields: list[Any] | None = None) -> dict[str, JsonValue]:
        view: dict[str, JsonValue] = {
            "gid": transfer.gid,
            "status": transfer.status,
            "totalLength": str(transfer.total_length),
            "completedLength": str(transfer.completed_length),
            "uploadLength": "0",
            "downloadSpeed": str(transfer.download_speed),
            "uploadSpeed": "0",
            "connections": str(len(transfer.peer_urls)),
            "numSeeders": str(len(transfer.peer_urls)),
            "seeder": "true" if transfer.status == "complete" else "false",
            "dir": str(transfer.output_path.parent),
            "files": self._file_views(transfer),
            "bitfield": _bitfield(transfer.completed_pieces, transfer.num_pieces),
            "numPieces": str(transfer.num_pieces),
            "pieceLength": str(transfer.piece_length),
            "errorCode": transfer.error_code,
            "errorMessage": transfer.error_message,
            "verifiedLength": str(transfer.completed_length),
            "verifyIntegrityPending": "false",
            "infoHash": transfer.manifest.root_hash[:40],
            "bittorrent": {
                "announceList": [],
                "creationDate": "0",
                "mode": "multi" if transfer.manifest.root_kind == "directory" else "single",
                "info": {"name": transfer.manifest.name},
            },
        }
        if fields:
            return {str(field): view[str(field)] for field in fields if str(field) in view}
        return view

    def _file_views(self, transfer: Transfer) -> list[dict[str, JsonValue]]:
        total = max(transfer.total_length, 1)
        completed = transfer.completed_length
        views: list[dict[str, JsonValue]] = []
        for index, file in enumerate(transfer.manifest.files, start=1):
            share = file.size / total
            file_completed = (
                min(file.size, int(completed * share))
                if transfer.status != "complete"
                else file.size
            )
            if transfer.manifest.root_kind == "file":
                path = str(transfer.output_path)
            else:
                path = str(transfer.output_path / file.path)
            views.append(
                {
                    "index": str(index),
                    "path": path,
                    "length": str(file.size),
                    "completedLength": str(file_completed),
                    "selected": "true",
                    "uris": [{"uri": transfer.uri, "status": "used"}],
                }
            )
        return views

    async def _telemetry_task_views(
        self,
        *,
        statuses: list[str] | None = None,
        fields: list[Any] | None = None,
    ) -> list[dict[str, JsonValue]]:
        snapshot = await self._telemetry_snapshot()
        if snapshot is None or not snapshot.enabled:
            return []
        views = [
            self._telemetry_task_view(snapshot, progress, fields=fields)
            for progress in snapshot.progress
        ]
        if statuses is not None:
            allowed = set(statuses)
            views = [view for view in views if str(view.get("status")) in allowed]
        return views

    async def _telemetry_task_by_gid(
        self,
        gid: str,
        *,
        fields: list[Any] | None = None,
    ) -> dict[str, JsonValue] | None:
        for task in await self._telemetry_task_views(fields=fields):
            if task.get("gid") == gid:
                return task
        return None

    async def _telemetry_gid_exists(self, gid: str) -> bool:
        return await self._telemetry_task_by_gid(gid) is not None

    async def _telemetry_files_by_gid(self, gid: str) -> list[dict[str, JsonValue]] | None:
        snapshot = await self._telemetry_snapshot()
        if snapshot is None or not snapshot.enabled:
            return None
        for progress in snapshot.progress:
            if _telemetry_gid(progress.id) == gid:
                return _telemetry_file_views(snapshot, progress)
        return None

    async def _telemetry_peers_by_gid(self, gid: str) -> list[dict[str, JsonValue]] | None:
        snapshot = await self._telemetry_snapshot()
        if snapshot is None or not snapshot.enabled:
            return None
        progress = next((row for row in snapshot.progress if _telemetry_gid(row.id) == gid), None)
        if progress is None:
            return None
        completed, total = _scaled_progress(progress.current, progress.total)
        bitfield = _bitfield(_scaled_pieces(completed, total), max(1, min(64, total)))
        return [
            {
                "peerId": f"bitswarm-workload-{index}",
                "ip": member.label,
                "port": "0",
                "client": f"{member.role or 'worker'}:{member.state}",
                "bitfield": bitfield,
                "amChoking": "false",
                "peerChoking": "false",
                "downloadSpeed": "0",
                "uploadSpeed": "0",
                "seeder": "true" if member.state.lower() in {"complete", "completed", "done"} else "false",
            }
            for index, member in enumerate(snapshot.members, start=1)
        ]

    async def _telemetry_snapshot(self) -> WorkloadTelemetry | None:
        if self._telemetry_provider is None:
            return None
        try:
            return await self._telemetry_provider.snapshot()
        except Exception as exc:
            return WorkloadTelemetry(
                enabled=True,
                title="Bitswarm telemetry",
                subtitle="Telemetry source returned an error.",
                workload_type="telemetry",
                status="error",
                phase="error",
                progress=[
                    TelemetryProgress(
                        id="telemetry-error",
                        label="Telemetry source error",
                        state="error",
                        current=0,
                        total=1,
                        unit="errors",
                        detail=str(exc),
                    )
                ],
            )

    def _telemetry_task_view(
        self,
        snapshot: WorkloadTelemetry,
        progress: TelemetryProgress,
        *,
        fields: list[Any] | None = None,
    ) -> dict[str, JsonValue]:
        completed, total = _scaled_progress(progress.current, progress.total)
        num_pieces = max(1, min(64, total))
        completed_pieces = _scaled_pieces(completed, total)
        gid = _telemetry_gid(progress.id)
        status = _telemetry_status(progress.state)
        speed = _rate_to_int(progress.rate)
        mode = "multi" if snapshot.members or snapshot.streams or snapshot.events else "single"
        view: dict[str, JsonValue] = {
            "gid": gid,
            "status": status,
            "totalLength": str(total),
            "completedLength": str(completed),
            "uploadLength": "0",
            "downloadSpeed": str(speed if status == "active" else 0),
            "uploadSpeed": "0",
            "connections": str(len(snapshot.members)),
            "numSeeders": "0",
            "seeder": "true" if status == "complete" else "false",
            "dir": f"bitswarm://{snapshot.workload_type or 'workload'}",
            "files": _telemetry_file_views(snapshot, progress),
            "bitfield": _bitfield(completed_pieces, num_pieces),
            "numPieces": str(num_pieces),
            "pieceLength": str(max(1, total // num_pieces)),
            "errorCode": "1" if status == "error" else "0",
            "errorMessage": progress.detail if status == "error" else "",
            "verifiedLength": str(completed),
            "verifyIntegrityPending": "false",
            "infoHash": gid * 2 + gid[:8],
            "bittorrent": {
                "announceList": [],
                "creationDate": "0",
                "mode": mode,
                "info": {"name": f"{snapshot.title} - {progress.label}"},
            },
            "comment": " | ".join(
                part
                for part in [
                    snapshot.subtitle,
                    f"{progress.state} {progress.current}/{progress.total} {progress.unit}",
                    progress.detail,
                ]
                if part
            ),
        }
        return _filter_view(view, fields)

    async def _run_task_views(
        self,
        *,
        statuses: list[str] | None = None,
        fields: list[Any] | None = None,
    ) -> list[dict[str, JsonValue]]:
        if self._run_registry is None:
            return []
        runs = await self._run_registry.list_runs()
        views = [self._run_task_view(run, fields=fields) for run in runs]
        if statuses is not None:
            allowed = set(statuses)
            views = [view for view in views if str(view.get("status")) in allowed]
        return views

    async def _run_task_by_gid(
        self,
        gid: str,
        *,
        fields: list[Any] | None = None,
    ) -> dict[str, JsonValue] | None:
        run = await self._run_by_gid(gid)
        if run is None:
            return None
        return self._run_task_view(run, fields=fields)

    async def _run_gid_exists(self, gid: str) -> bool:
        return await self._run_by_gid(gid) is not None

    async def _run_files_by_gid(self, gid: str) -> list[dict[str, JsonValue]] | None:
        run = await self._run_by_gid(gid)
        if run is None:
            return None
        return _run_file_views(run)

    async def _run_peers_by_gid(self, gid: str) -> list[dict[str, JsonValue]] | None:
        run = await self._run_by_gid(gid)
        if run is None:
            return None
        bitfield = _bitfield(len(run.members), max(1, int(run.settings.get("max_workers", 1))))
        return [
            {
                "peerId": f"bitswarm-run-{member.actor}",
                "ip": member.actor,
                "port": "0",
                "client": f"{member.role}:{member.state}",
                "bitfield": bitfield,
                "amChoking": "false",
                "peerChoking": "false",
                "downloadSpeed": "0",
                "uploadSpeed": "0",
                "seeder": "true" if member.role == "host" else "false",
            }
            for member in run.members
        ]

    async def _run_by_gid(self, gid: str) -> RunRecord | None:
        if self._run_registry is None:
            return None
        for run in await self._run_registry.list_runs():
            if _run_gid(run.run_id) == gid:
                return run
        return None

    def _run_task_view(
        self,
        run: RunRecord,
        *,
        fields: list[Any] | None = None,
    ) -> dict[str, JsonValue]:
        total = max(1, int(run.settings.get("max_workers", 1)))
        completed = min(len(run.members), total)
        status = _run_status(run.status)
        gid = _run_gid(run.run_id)
        view: dict[str, JsonValue] = {
            "gid": gid,
            "status": status,
            "totalLength": str(total),
            "completedLength": str(completed),
            "uploadLength": "0",
            "downloadSpeed": "0",
            "uploadSpeed": "0",
            "connections": str(len(run.members)),
            "numSeeders": "1",
            "seeder": "true" if status == "complete" else "false",
            "dir": f"bitswarm://runs/{run.run_id}",
            "files": _run_file_views(run),
            "bitfield": _bitfield(completed, total),
            "numPieces": str(total),
            "pieceLength": "1",
            "errorCode": "1" if status == "error" else "0",
            "errorMessage": "",
            "verifiedLength": str(completed),
            "verifyIntegrityPending": "false",
            "infoHash": gid * 2 + gid[:8],
            "bittorrent": {
                "announceList": [],
                "creationDate": str(run.created_at_ms // 1000),
                "mode": "multi",
                "info": {"name": f"{run.name} [{run.recipe_label}]"},
            },
            "comment": " | ".join(
                [
                    f"host {run.host_actor}",
                    run.profile_label,
                    run.visibility,
                    f"{len(run.members)}/{total} joined",
                ]
            ),
        }
        return _filter_view(view, fields)


class RpcFailure(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


async def _default_download(
    manifest: BitswarmManifest,
    peer_urls: list[PeerInput],
    output_path: Path,
    progress_cb: ProgressCallback | None,
) -> Path:
    return await download_manifest(
        manifest,
        peer_urls=peer_urls,
        output_path=output_path,
        progress_cb=progress_cb,
    )


async def _parse_bitswarm_uri(uri: str, *, options: dict[str, Any]) -> ParsedBitswarmUri:
    parsed = urlparse(uri)
    query = parse_qs(parsed.query, keep_blank_values=False)
    if parsed.scheme == "magnet":
        xt_values = [unquote(value) for value in query.get("xt", [])]
        if xt_values and not any(value.startswith("urn:bitswarm:") for value in xt_values):
            raise RpcFailure(-32602, "magnet URI must contain xt=urn:bitswarm:<manifest-id>")
        manifest_values = query.get("xs", []) or query.get("manifest", [])
        if not manifest_values:
            raise RpcFailure(-32602, "Bitswarm magnet URI requires xs=<manifest source>")
        manifest_ref = unquote(manifest_values[0])
        peer_urls = [
            unquote(value)
            for value in [*query.get("x.pe", []), *query.get("peer", [])]
        ]
        output_values = query.get("x.out", []) or query.get("out", []) or query.get("output", [])
        output_path = Path(unquote(output_values[0])).expanduser() if output_values else None
        return ParsedBitswarmUri(
            manifest_ref=manifest_ref,
            peer_urls=peer_urls,
            output_path=output_path,
            tracker_url=_first_query_value(query, "tr") or _first_query_value(query, "tracker"),
            tracker_token=_first_query_value(query, "x.token") or _first_query_value(query, "token"),
        )
    if parsed.scheme in {"bitswarm", "bitswarm+file"}:
        manifest_values = query.get("manifest")
        if not manifest_values:
            raise RpcFailure(-32602, "bitswarm URI requires a manifest query parameter")
        manifest_ref = unquote(manifest_values[0])
        peer_urls = [unquote(value) for value in query.get("peer", [])]
        output_values = query.get("out", []) or query.get("output", [])
        output_path = Path(unquote(output_values[0])).expanduser() if output_values else None
        return ParsedBitswarmUri(
            manifest_ref=manifest_ref,
            peer_urls=peer_urls,
            output_path=output_path,
            tracker_url=_first_query_value(query, "tracker"),
            tracker_token=_first_query_value(query, "token"),
        )
    if parsed.scheme == "file":
        manifest_ref = unquote(parsed.path)
        peer_urls = [unquote(value) for value in query.get("peer", [])]
        output_values = query.get("out", []) or query.get("output", [])
        output_path = Path(unquote(output_values[0])).expanduser() if output_values else None
        return ParsedBitswarmUri(manifest_ref=manifest_ref, peer_urls=peer_urls, output_path=output_path)
    if parsed.scheme in {"http", "https"}:
        peer_urls = [unquote(value) for value in query.get("peer", [])]
        output_values = query.get("out", []) or query.get("output", [])
        output_path = Path(unquote(output_values[0])).expanduser() if output_values else None
        return ParsedBitswarmUri(manifest_ref=uri, peer_urls=peer_urls, output_path=output_path)
    path = Path(uri).expanduser()
    if path.exists():
        peer_option = options.get("peer") or options.get("bitswarm-peer")
        peer_urls = _option_list(peer_option)
        return ParsedBitswarmUri(manifest_ref=str(path), peer_urls=peer_urls, output_path=None)
    raise RpcFailure(
        -32602,
        "expected bitswarm:?manifest=...&peer=... URI, file:// manifest URI, HTTP(S) manifest URI, "
        "or existing local manifest path",
    )


async def _load_manifest_ref(ref: str) -> BitswarmManifest:
    parsed = urlparse(ref)
    if parsed.scheme in {"http", "https"}:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(ref)
            response.raise_for_status()
            return BitswarmManifest.model_validate(response.json())
    if parsed.scheme == "file":
        return load_manifest(Path(unquote(parsed.path)).expanduser())
    return load_manifest(Path(ref).expanduser())


def _output_path_from_options(
    options: dict[str, Any],
    *,
    default_dir: Path,
    manifest: BitswarmManifest,
) -> Path:
    directory = Path(str(options.get("dir") or default_dir)).expanduser()
    name = str(options.get("out") or manifest.name)
    return directory / name


def _first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return unquote(values[0])


def _option_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _fields_from_tail(params: list[Any]) -> list[Any] | None:
    if params and isinstance(params[-1], list):
        return params[-1]
    return None


def _gid_from_params(params: list[Any]) -> str:
    if not params or not isinstance(params[0], str):
        raise RpcFailure(-32602, "gid is required")
    return params[0]


def _strip_token(params: list[Any]) -> list[Any]:
    if params and isinstance(params[0], str) and params[0].startswith("token:"):
        return params[1:]
    return params


def _coerce_json_value(value: Any) -> JsonValue:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list):
        return [_coerce_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _coerce_json_value(item) for key, item in value.items()}
    return str(value)


def _filter_view(view: dict[str, JsonValue], fields: list[Any] | None) -> dict[str, JsonValue]:
    if not fields:
        return view
    return {str(field): view[str(field)] for field in fields if str(field) in view}


def _bitfield(completed_pieces: int, total_pieces: int) -> str:
    if total_pieces <= 0:
        return ""
    bits = ["1" if index < completed_pieces else "0" for index in range(total_pieces)]
    while len(bits) % 4:
        bits.append("0")
    return "".join(f"{int(''.join(bits[index:index + 4]), 2):x}" for index in range(0, len(bits), 4))


def _telemetry_gid(progress_id: str) -> str:
    return hashlib.blake2s(f"telemetry:{progress_id}".encode(), digest_size=8).hexdigest()


def _run_gid(run_id: str) -> str:
    return hashlib.blake2s(f"run:{run_id}".encode(), digest_size=8).hexdigest()


def _telemetry_status(state: str) -> str:
    normalized = state.strip().lower().replace("_", "-")
    if normalized in {"complete", "completed", "done", "accepted", "success", "succeeded"}:
        return "complete"
    if normalized in {"waiting", "queued", "pending", "idle"}:
        return "waiting"
    if normalized in {"paused", "suspended"}:
        return "paused"
    if normalized in {"error", "failed", "failure", "rejected"}:
        return "error"
    if normalized in {"removed", "cancelled", "canceled"}:
        return "removed"
    return "active"


def _run_status(state: str) -> str:
    normalized = state.strip().lower()
    if normalized == "running":
        return "active"
    if normalized == "paused":
        return "paused"
    if normalized == "complete":
        return "complete"
    if normalized == "error":
        return "error"
    return "waiting"


def _scaled_progress(current: int | float, total: int | float) -> tuple[int, int]:
    current_f = max(float(current), 0.0)
    total_f = max(float(total), 1e-9)
    scale = 1000 if not current_f.is_integer() or not total_f.is_integer() else 1
    scaled_total = max(1, int(round(total_f * scale)))
    scaled_current = max(0, min(scaled_total, int(round(current_f * scale))))
    return scaled_current, scaled_total


def _scaled_pieces(completed: int, total: int) -> int:
    total_pieces = max(1, min(64, total))
    if total <= 0:
        return 0
    return max(0, min(total_pieces, int(round(total_pieces * (completed / total)))))


def _rate_to_int(value: str) -> int:
    match = re.search(r"[-+]?\d+(?:\.\d+)?", value)
    if match is None:
        return 0
    return max(0, int(float(match.group(0))))


def _telemetry_file_views(
    snapshot: WorkloadTelemetry,
    progress: TelemetryProgress,
) -> list[dict[str, JsonValue]]:
    completed, total = _scaled_progress(progress.current, progress.total)
    rows: list[tuple[str, int, int]] = [
        (
            _display_path(snapshot.title, "progress", progress.label, progress.detail or progress.state),
            total,
            completed,
        )
    ]
    for metric in snapshot.metrics:
        rows.append(
            (
                _display_path(
                    snapshot.title,
                    "metric",
                    metric.label,
                    f"{metric.value} {metric.detail}".strip(),
                ),
                1,
                1,
            )
        )
    for member in snapshot.members:
        member_completed, member_total = _optional_scaled_pair(member.current, member.total)
        rows.append(
            (
                _display_path(
                    snapshot.title,
                    "member",
                    member.label,
                    " ".join(part for part in [member.role, member.state, member.detail] if part),
                ),
                member_total,
                member_completed,
            )
        )
    for stream in snapshot.streams:
        stream_completed, stream_total = _optional_scaled_pair(stream.current, stream.total)
        stream_detail = " ".join(
            part for part in [stream.kind, stream.state, stream.score, stream.detail] if part
        )
        rows.append(
            (
                _display_path(snapshot.title, "stream", stream.label, stream_detail),
                stream_total,
                stream_completed,
            )
        )
        if stream.prompt:
            rows.append(
                (
                    _display_path(snapshot.title, "stream", f"{stream.label} prompt", stream.prompt),
                    1,
                    1,
                )
            )
        if stream.output:
            rows.append(
                (
                    _display_path(snapshot.title, "stream", f"{stream.label} output", stream.output),
                    1,
                    1,
                )
            )
    for event in snapshot.events[-10:]:
        rows.append((_display_path(snapshot.title, "event", event.level, event.message), 1, 1))
    return [
        {
            "index": str(index),
            "path": path,
            "length": str(max(length, 1)),
            "completedLength": str(max(0, min(completed_length, max(length, 1)))),
            "selected": "true",
            "uris": [{"uri": f"bitswarm-telemetry:{progress.id}", "status": "used"}],
        }
        for index, (path, length, completed_length) in enumerate(rows, start=1)
    ]


def _optional_scaled_pair(
    current: int | float | None,
    total: int | float | None,
) -> tuple[int, int]:
    if current is None or total is None:
        return 1, 1
    return _scaled_progress(current, total)


def _display_path(title: str, section: str, label: str, detail: str = "") -> str:
    parts = [_safe_path_part(title), _safe_path_part(section), _safe_path_part(label)]
    if detail:
        parts.append(_safe_path_part(detail))
    return "/".join(part for part in parts if part)


def _safe_path_part(value: str) -> str:
    return " ".join(value.replace("/", " / ").split())


def _run_file_views(run: RunRecord) -> list[dict[str, JsonValue]]:
    total = max(1, int(run.settings.get("max_workers", 1)))
    pending = sum(1 for seed in run.seeds if seed.state == "pending")
    leased = sum(1 for seed in run.seeds if seed.state == "leased")
    completed = sum(1 for seed in run.seeds if seed.state == "completed")
    rows: list[tuple[str, int, int]] = [
        (
            _display_path(run.name, "run", run.run_id, f"{run.status} {len(run.members)}/{total} joined"),
            total,
            min(len(run.members), total),
        ),
        (
            _display_path(
                run.name,
                "seeds",
                "summary",
                f"pending {pending} leased {leased} completed {completed}",
            ),
            max(1, len(run.seeds)),
            completed,
        ),
        (_display_path(run.name, "recipe", run.recipe_label, run.recipe_id), 1, 1),
        (_display_path(run.name, "profile", run.profile_label, run.profile_id), 1, 1),
        (_display_path(run.name, "host", run.host_actor, run.visibility), 1, 1),
    ]
    for key, value in sorted(run.settings.items()):
        rows.append((_display_path(run.name, "setting", key, str(value)), 1, 1))
    for member in run.members:
        rows.append((_display_path(run.name, "member", member.actor, f"{member.role} {member.state}"), 1, 1))
    for seed in sorted(run.seeds, key=lambda row: (row.issued_at_ms, row.seed_id))[:32]:
        rows.append(
            (
                _display_path(
                    run.name,
                    "seed",
                    seed.seed_id,
                    f"{seed.state} {seed.sigma_id} issued {seed.issued_at_ms}",
                ),
                max(1, len(seed.rollouts)),
                sum(1 for rollout in seed.rollouts if rollout.status in {"completed", "failed"}),
            )
        )
        for rollout in seed.rollouts:
            correctness = (
                "pending"
                if rollout.correct is None
                else ("correct" if rollout.correct else "wrong")
            )
            rows.append(
                (
                    _display_path(
                        run.name,
                        "rollout",
                        f"{seed.seed_id} {rollout.item_id}",
                        f"{rollout.machine} {rollout.sign} {rollout.status} {correctness}",
                    ),
                    1,
                    1 if rollout.status in {"completed", "failed"} else 0,
                )
            )
    return [
        {
            "index": str(index),
            "path": path,
            "length": str(max(length, 1)),
            "completedLength": str(max(0, min(completed, max(length, 1)))),
            "selected": "true",
            "uris": [{"uri": f"bitswarm-run:{run.run_id}", "status": "used"}],
        }
        for index, (path, length, completed) in enumerate(rows, start=1)
    ]
