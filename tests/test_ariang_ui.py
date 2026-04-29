from __future__ import annotations

import asyncio
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
