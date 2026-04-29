from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pytest
import uvicorn

import bitswarm.cli as cli_module
from bitswarm.ariang.app import create_ariang_app
from bitswarm.client.downloader import PeerInput, ProgressCallback
from bitswarm.protocol.manifest import create_manifest, save_manifest
from bitswarm.protocol.schemas import BitswarmManifest


def _write_training_telemetry(tmp_path: Path) -> Path:
    telemetry_path = tmp_path / "telemetry.json"
    telemetry_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "title": "Local training testnet",
                "subtitle": "Qwen 0.5B local lane",
                "workload_type": "training",
                "status": "running",
                "phase": "evaluating",
                "updated_at_ms": 123,
                "metrics": [{"label": "score", "value": "0.42", "detail": "live"}],
                "progress": [
                    {
                        "id": "round",
                        "label": "Round population",
                        "state": "evaluating",
                        "current": 3,
                        "total": 5,
                        "unit": "seeds",
                        "detail": "worker packets",
                    },
                    {
                        "id": "validation",
                        "label": "Validator replay",
                        "state": "waiting",
                        "current": 0,
                        "total": 1,
                        "unit": "candidate",
                        "detail": "starts after shortlist",
                    },
                ],
                "members": [
                    {
                        "id": "worker-a",
                        "label": "worker-a",
                        "role": "proposer",
                        "state": "evaluating",
                        "detail": "job-0003",
                        "current": 8,
                        "total": 64,
                    }
                ],
                "streams": [
                    {
                        "id": "stream-a",
                        "label": "gsm8k_fast-0001",
                        "kind": "rollout",
                        "state": "decode",
                        "current": 48,
                        "total": 512,
                        "prompt": "Question: 2+2?",
                        "output": "4",
                        "score": "1.0",
                        "detail": "greedy",
                    }
                ],
                "events": [{"ts_ms": 123, "level": "info", "message": "round started"}],
            }
        ),
        encoding="utf-8",
    )
    return telemetry_path


async def test_ariang_serves_vendored_ui() -> None:
    app = create_ariang_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "<title>Bitswarm</title>" in response.text
    assert "bitswarm-adapter.js" in response.text
    assert "bitswarm-adapter.css" in response.text
    assert "aria-ng" in response.text


async def test_ariang_exposes_optional_workload_telemetry(tmp_path: Path) -> None:
    telemetry_path = _write_training_telemetry(tmp_path)
    app = create_ariang_app(telemetry_json=telemetry_path)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        response = await client.get("/api/bitswarm/ui/telemetry")
    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["workload_type"] == "training"
    assert payload["progress"][0]["current"] == 3
    assert payload["streams"][0]["output"] == "4"


async def test_ariang_projects_workload_telemetry_as_native_tasks(tmp_path: Path) -> None:
    app = create_ariang_app(telemetry_json=_write_training_telemetry(tmp_path))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        response = await client.post(
            "/jsonrpc",
            json={
                "jsonrpc": "2.0",
                "id": "active",
                "method": "aria2.tellActive",
                "params": [["gid", "status", "totalLength", "completedLength", "bittorrent", "files"]],
            },
        )
    assert response.status_code == 200
    rows = response.json()["result"]
    assert len(rows) == 1
    task = rows[0]
    assert task["status"] == "active"
    assert task["totalLength"] == "5"
    assert task["completedLength"] == "3"
    assert task["bittorrent"]["info"]["name"] == "Local training testnet - Round population"
    file_paths = [row["path"] for row in task["files"]]
    assert any("stream/gsm8k_fast-0001 output/4" in path for path in file_paths)


async def test_ariang_tell_status_and_files_for_native_workload_task(tmp_path: Path) -> None:
    app = create_ariang_app(telemetry_json=_write_training_telemetry(tmp_path))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        active_response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "active", "method": "aria2.tellActive", "params": []},
        )
        gid = active_response.json()["result"][0]["gid"]
        status_response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "status", "method": "aria2.tellStatus", "params": [gid]},
        )
        files_response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "files", "method": "aria2.getFiles", "params": [gid]},
        )
    task = status_response.json()["result"]
    assert task["gid"] == gid
    assert task["connections"] == "1"
    assert task["comment"].startswith("Qwen 0.5B local lane")
    files = files_response.json()["result"]
    assert any(row["path"].endswith("stream/gsm8k_fast-0001 prompt/Question: 2+2?") for row in files)


async def test_ariang_global_stat_counts_native_workload_tasks(tmp_path: Path) -> None:
    app = create_ariang_app(telemetry_json=_write_training_telemetry(tmp_path))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "stat", "method": "aria2.getGlobalStat", "params": []},
        )
    result = response.json()["result"]
    assert result["numActive"] == "1"
    assert result["numWaiting"] == "1"
    assert result["numStopped"] == "0"


async def test_ariang_default_telemetry_is_disabled() -> None:
    app = create_ariang_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        response = await client.get("/api/bitswarm/ui/telemetry")
    assert response.status_code == 200
    assert response.json()["enabled"] is False


async def test_ariang_jsonrpc_add_uri_download_tracks_completion(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    save_manifest(manifest, manifest_path)
    observed: dict[str, Any] = {}

    async def fake_download(
        manifest_arg: BitswarmManifest,
        peer_urls: list[PeerInput],
        output_path: Path,
        progress_cb: ProgressCallback | None,
    ) -> Path:
        observed["manifest_id"] = manifest_arg.manifest_id
        observed["peer_urls"] = peer_urls
        observed["output_path"] = output_path
        assert progress_cb is not None
        for index, piece in enumerate(manifest_arg.pieces, start=1):
            await progress_cb(index, len(manifest_arg.pieces), piece.piece_id)
        return output_path

    app = create_ariang_app(download_fn=fake_download, default_output_dir=tmp_path / "downloads")
    transport = httpx.ASGITransport(app=app)
    uri = (
        "bitswarm:?manifest="
        f"{quote(str(manifest_path))}"
        "&peer=http%3A%2F%2F127.0.0.1%3A8899"
        f"&out={quote(str(tmp_path / 'out'))}"
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        add_response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "add", "method": "aria2.addUri", "params": [[uri], {}]},
        )
        assert add_response.status_code == 200
        gid = add_response.json()["result"]
        for _ in range(20):
            await asyncio.sleep(0.01)
            status_response = await client.post(
                "/jsonrpc",
                json={
                    "jsonrpc": "2.0",
                    "id": "status",
                    "method": "aria2.tellStatus",
                    "params": [gid],
                },
            )
            task = status_response.json()["result"]
            if task["status"] == "complete":
                break
        else:
            raise AssertionError("download did not complete")

    assert observed["manifest_id"] == manifest.manifest_id
    assert observed["peer_urls"] == ["http://127.0.0.1:8899"]
    assert task["completedLength"] == str(manifest.total_size)
    assert task["numPieces"] == str(len(manifest.pieces))
    assert task["bittorrent"]["info"]["name"] == manifest.name


async def test_ariang_jsonrpc_add_uri_accepts_bitswarm_magnet(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    save_manifest(manifest, manifest_path)
    observed: dict[str, Any] = {}

    async def fake_download(
        manifest_arg: BitswarmManifest,
        peer_urls: list[PeerInput],
        output_path: Path,
        progress_cb: ProgressCallback | None,
    ) -> Path:
        del progress_cb
        observed["manifest_id"] = manifest_arg.manifest_id
        observed["peer_urls"] = peer_urls
        observed["output_path"] = output_path
        return output_path

    app = create_ariang_app(download_fn=fake_download, default_output_dir=tmp_path / "downloads")
    transport = httpx.ASGITransport(app=app)
    uri = (
        f"magnet:?xt=urn:bitswarm:{manifest.manifest_id}"
        f"&xs={quote(str(manifest_path))}"
        "&x.pe=http%3A%2F%2F127.0.0.1%3A8899"
        f"&x.out={quote(str(tmp_path / 'magnet-out'))}"
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        add_response = await client.post(
            "/jsonrpc",
            json={"jsonrpc": "2.0", "id": "add", "method": "aria2.addUri", "params": [[uri], {}]},
        )
        assert add_response.status_code == 200
        gid = add_response.json()["result"]
        for _ in range(20):
            await asyncio.sleep(0.01)
            status_response = await client.post(
                "/jsonrpc",
                json={"jsonrpc": "2.0", "id": "status", "method": "aria2.tellStatus", "params": [gid]},
            )
            task = status_response.json()["result"]
            if task["status"] == "complete":
                break
        else:
            raise AssertionError("download did not complete")

    assert observed["manifest_id"] == manifest.manifest_id
    assert observed["peer_urls"] == ["http://127.0.0.1:8899"]
    assert observed["output_path"] == tmp_path / "magnet-out"


async def test_ariang_jsonrpc_multicall_and_global_stat(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    save_manifest(manifest, manifest_path)

    async def fake_download(
        manifest_arg: BitswarmManifest,
        peer_urls: list[PeerInput],
        output_path: Path,
        progress_cb: ProgressCallback | None,
    ) -> Path:
        del manifest_arg, peer_urls, output_path, progress_cb
        return tmp_path / "out"

    app = create_ariang_app(download_fn=fake_download)
    transport = httpx.ASGITransport(app=app)
    uri = f"bitswarm:?manifest={quote(str(manifest_path))}&peer=http%3A%2F%2F127.0.0.1%3A8899"
    async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
        gid = (
            await client.post(
                "/jsonrpc",
                json={"jsonrpc": "2.0", "id": "add", "method": "aria2.addUri", "params": [[uri], {}]},
            )
        ).json()["result"]
        response = await client.post(
            "/jsonrpc",
            json={
                "jsonrpc": "2.0",
                "id": "multi",
                "method": "system.multicall",
                "params": [
                    [
                        {"methodName": "aria2.tellStatus", "params": [gid]},
                        {"methodName": "aria2.getGlobalStat", "params": []},
                    ]
                ],
            },
        )
    result = response.json()["result"]
    assert result[0][0]["gid"] == gid
    assert set(result[1][0]) >= {"numActive", "numWaiting", "numStopped", "downloadSpeed"}


def test_cli_webui_rejects_remote_bind_without_explicit_unsafe_flag() -> None:
    with pytest.raises(SystemExit):
        cli_module.main(["webui", "--host", "0.0.0.0"])


def test_cli_webui_runs_vendored_ariang(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: object, *, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr(uvicorn, "run", fake_run)
    assert cli_module.main(["webui", "--host", "127.0.0.1", "--port", "8897"]) == 0
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8897
    assert captured["app"].state.bitswarm_ariang_bridge is not None


def test_cli_webui_rejects_two_telemetry_sources() -> None:
    with pytest.raises(SystemExit):
        cli_module.main(
            [
                "webui",
                "--telemetry-json",
                "telemetry.json",
                "--telemetry-url",
                "http://127.0.0.1:9000/status",
            ]
        )
