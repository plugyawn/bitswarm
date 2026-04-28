"""FastAPI application for the local Bitswarm Web UI."""

from __future__ import annotations

from importlib import resources
from typing import Annotated

from fastapi import FastAPI, HTTPException
from fastapi import Path as RoutePath
from fastapi.responses import HTMLResponse, Response

from bitswarm.constants import CONTROL_ID_PATTERN, MAX_ID_LENGTH
from bitswarm.protocol.schemas import BitswarmManifest, BitswarmPieceMap

from .models import (
    WebUIAnnounceCreate,
    WebUIDownloadCreate,
    WebUIMessage,
    WebUISeedCreate,
    WebUISeedView,
    WebUIStateView,
    WebUITransferView,
)
from .state import DownloadFn, WebUIState

PathControlId = Annotated[
    str,
    RoutePath(min_length=1, max_length=MAX_ID_LENGTH, pattern=CONTROL_ID_PATTERN),
]


def create_webui_app(*, download_fn: DownloadFn | None = None) -> FastAPI:
    state = WebUIState(download_fn=download_fn) if download_fn is not None else WebUIState()
    app = FastAPI(title="bitswarm-webui", version="1.0.0a1")
    app.state.bitswarm_webui_state = state

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return _static_text("index.html")

    @app.get("/assets/{asset_name}")
    async def asset(asset_name: str) -> Response:
        if asset_name not in {"app.js", "styles.css"}:
            raise HTTPException(status_code=404, detail="asset not found")
        media_type = "text/javascript" if asset_name.endswith(".js") else "text/css"
        return Response(_static_text(asset_name), media_type=media_type)

    @app.get("/api/ui/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/health")
    async def peer_health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/ui/state", response_model=WebUIStateView)
    async def ui_state() -> WebUIStateView:
        return state.snapshot()

    @app.post("/api/ui/transfers/download", response_model=WebUITransferView)
    async def add_download(request: WebUIDownloadCreate) -> WebUITransferView:
        try:
            return await state.add_download(request)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/api/ui/transfers/{transfer_id}/start", response_model=WebUITransferView)
    async def start_download(transfer_id: PathControlId) -> WebUITransferView:
        try:
            return state.start_download(transfer_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="transfer not found") from exc

    @app.post("/api/ui/transfers/{transfer_id}/cancel", response_model=WebUITransferView)
    async def cancel_download(transfer_id: PathControlId) -> WebUITransferView:
        try:
            return await state.cancel_download(transfer_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="transfer not found") from exc

    @app.post("/api/ui/seeds", response_model=WebUISeedView)
    async def add_seed(request: WebUISeedCreate) -> WebUISeedView:
        try:
            return await state.add_seed(request)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.delete("/api/ui/seeds/{seed_id}", response_model=WebUIMessage)
    async def remove_seed(seed_id: PathControlId) -> WebUIMessage:
        try:
            return await state.remove_seed(seed_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="seed not found") from exc

    @app.post("/api/ui/seeds/{seed_id}/announce", response_model=WebUIMessage)
    async def announce_seed(seed_id: PathControlId, request: WebUIAnnounceCreate) -> WebUIMessage:
        try:
            return await state.announce_seed(seed_id, request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="seed not found") from exc
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/api/manifests/{manifest_id}", response_model=BitswarmManifest)
    async def get_manifest(manifest_id: PathControlId) -> BitswarmManifest:
        try:
            return state.seeded_manifest(manifest_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="manifest not found") from exc

    @app.get("/api/manifests/{manifest_id}/piece-map", response_model=BitswarmPieceMap)
    async def piece_map(manifest_id: PathControlId) -> BitswarmPieceMap:
        try:
            return state.seeded_piece_map(manifest_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="manifest not found") from exc

    @app.get("/api/manifests/{manifest_id}/pieces/{piece_id}")
    async def get_piece(manifest_id: PathControlId, piece_id: PathControlId) -> Response:
        try:
            data = state.seeded_piece(manifest_id, piece_id)
            piece = next(
                item for item in state.seeded_manifest(manifest_id).pieces if item.piece_id == piece_id
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="piece not found") from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Bitswarm-Manifest-Id": manifest_id,
                "X-Bitswarm-Piece-Id": piece_id,
                "X-Bitswarm-Piece-Sha256": piece.sha256,
            },
        )

    return app


def _static_text(name: str) -> str:
    return resources.files("bitswarm.webui.static").joinpath(name).read_text(encoding="utf-8")


def is_safe_local_bind(host: str) -> bool:
    return host in {"127.0.0.1", "::1", "localhost"}
