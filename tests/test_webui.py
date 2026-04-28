from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from bitswarm.cli import main
from bitswarm.protocol.manifest import create_manifest, save_manifest
from bitswarm.protocol.pieces import read_piece
from bitswarm.webui.app import create_webui_app


async def test_webui_serves_static_console() -> None:
    app = create_webui_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ui") as client:
        response = await client.get("/")
        assert response.status_code == 200
        assert "Bitswarm" in response.text
        assert "Start download" in response.text
        stylesheet = await client.get("/assets/styles.css")
        assert stylesheet.status_code == 200
        assert "torrent-style" in response.text


async def test_webui_seed_exposes_bitswarm_peer_endpoints(sample_tree: Path) -> None:
    app = create_webui_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ui") as client:
        seed_response = await client.post(
            "/api/ui/seeds",
            json={"root_path": str(sample_tree), "piece_size": 9, "name": "sample"},
        )
        assert seed_response.status_code == 200
        seed = seed_response.json()

        manifest_response = await client.get(f"/api/manifests/{seed['manifest_id']}")
        assert manifest_response.status_code == 200
        manifest = manifest_response.json()
        assert manifest["name"] == "sample"

        piece_map_response = await client.get(f"/api/manifests/{seed['manifest_id']}/piece-map")
        assert piece_map_response.status_code == 200
        piece_ids = piece_map_response.json()["piece_ids"]
        assert piece_ids

        first_piece = manifest["pieces"][0]
        piece_response = await client.get(
            f"/api/manifests/{seed['manifest_id']}/pieces/{first_piece['piece_id']}"
        )
        assert piece_response.status_code == 200
        assert piece_response.headers["X-Bitswarm-Piece-Id"] == first_piece["piece_id"]

        parsed_manifest = create_manifest(sample_tree, piece_size=9, name="sample")
        assert piece_response.content == read_piece(sample_tree, parsed_manifest.pieces[0])


async def test_webui_download_tracks_progress(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    manifest_path = tmp_path / "manifest.json"
    save_manifest(manifest, manifest_path)

    async def fake_download(*args: Any, **kwargs: Any) -> Path:
        progress_cb = kwargs["progress_cb"]
        for index, piece in enumerate(manifest.pieces, start=1):
            maybe_awaitable = progress_cb(index, len(manifest.pieces), piece.piece_id)
            if maybe_awaitable is not None:
                await maybe_awaitable
            await asyncio.sleep(0)
        return Path(kwargs["output_path"])

    app = create_webui_app(download_fn=fake_download)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ui") as client:
        response = await client.post(
            "/api/ui/transfers/download",
            json={
                "manifest_path": str(manifest_path),
                "output_path": str(tmp_path / "downloaded"),
                "peers": ["http://peer"],
                "auto_start": True,
            },
        )
        assert response.status_code == 200
        transfer_id = response.json()["transfer_id"]

        completed = None
        for _ in range(20):
            snapshot = (await client.get("/api/ui/state")).json()
            completed = next(item for item in snapshot["transfers"] if item["transfer_id"] == transfer_id)
            if completed["status"] == "completed":
                break
            await asyncio.sleep(0.01)

        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["completed_pieces"] == len(manifest.pieces)
        assert completed["completed_bytes"] == manifest.total_size
        assert completed["progress"] == 1.0
        assert all(piece["status"] == "done" for piece in completed["pieces"])


def test_cli_webui_rejects_remote_bind_without_explicit_unsafe_flag() -> None:
    with pytest.raises(SystemExit):
        main(["webui", "--host", "0.0.0.0"])


def test_cli_webui_runs_on_local_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: object, *, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr(uvicorn, "run", fake_run)
    assert main(["webui", "--host", "127.0.0.1", "--port", "8897"]) == 0
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8897
    assert captured["app"].state.bitswarm_webui_state is not None
