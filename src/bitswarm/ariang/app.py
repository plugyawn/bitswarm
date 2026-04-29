"""FastAPI application serving the vendored AriaNg UI."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .rpc import AriaNgBridge, DownloadFn
from .telemetry import TelemetryProvider


def create_ariang_app(
    *,
    download_fn: DownloadFn | None = None,
    default_output_dir: Path | None = None,
    telemetry_json: Path | None = None,
    telemetry_url: str | None = None,
) -> FastAPI:
    """Create a local AriaNg UI backed by a Bitswarm JSON-RPC bridge."""
    static_root = Path(__file__).parent / "vendor" / "ariang"
    telemetry = TelemetryProvider(json_path=telemetry_json, url=telemetry_url)
    state = AriaNgBridge(
        download_fn=download_fn,
        default_output_dir=default_output_dir,
        telemetry_provider=telemetry,
    )
    app = FastAPI(title="bitswarm-ariang", version="1.0.0a1")
    app.state.bitswarm_ariang_bridge = state
    app.state.bitswarm_telemetry_provider = telemetry

    @app.get("/api/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/bitswarm/ui/telemetry")
    async def ui_telemetry() -> dict[str, object]:
        return (await telemetry.snapshot()).model_dump(mode="json")

    @app.post("/jsonrpc")
    async def jsonrpc(request: Request) -> JSONResponse:
        payload = await request.json()
        if isinstance(payload, list):
            return JSONResponse([await state.handle_jsonrpc(item) for item in payload])
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="JSON-RPC payload must be an object or array")
        return JSONResponse(await state.handle_jsonrpc(payload))

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(static_root / "index.html")

    app.mount("/", StaticFiles(directory=static_root, html=True), name="ariang")
    return app


def is_safe_local_bind(host: str) -> bool:
    """Return whether a UI bind target is loopback-only."""
    return host in {"127.0.0.1", "::1", "localhost"}
