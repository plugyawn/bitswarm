"""Local seeder application."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException
from fastapi import Path as RoutePath
from fastapi.responses import Response

from bitswarm.constants import CONTROL_ID_PATTERN, MAX_ID_LENGTH
from bitswarm.protocol.manifest import create_manifest
from bitswarm.protocol.paths import resolve_root_without_symlinks
from bitswarm.protocol.peer import full_piece_map
from bitswarm.protocol.pieces import (
    directory_identity_no_symlinks,
    file_identity_no_symlinks,
    piece_by_id,
    read_piece,
)
from bitswarm.protocol.schemas import BitswarmManifest, BitswarmPieceMap
from bitswarm.protocol.verifier import verify_manifest_tree, verify_piece_bytes

PathControlId = Annotated[
    str,
    RoutePath(min_length=1, max_length=MAX_ID_LENGTH, pattern=CONTROL_ID_PATTERN),
]


def create_seeder_app(
    root: Path,
    *,
    manifest: BitswarmManifest | None = None,
    piece_size: int | None = None,
) -> FastAPI:
    resolved_root = resolve_root_without_symlinks(root)
    if manifest is None:
        kwargs = {"piece_size": piece_size} if piece_size is not None else {}
        manifest = create_manifest(root, **kwargs)
    else:
        verify_manifest_tree(root, manifest)
    root_identity = _root_identity(resolved_root, manifest)
    app = FastAPI(title="bitswarm-seeder", version="1.0.0a1")

    @app.get("/api/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/manifests/{manifest_id}", response_model=BitswarmManifest)
    async def get_manifest(manifest_id: PathControlId) -> BitswarmManifest:
        if manifest_id != manifest.manifest_id:
            raise HTTPException(status_code=404, detail="manifest not found")
        return manifest

    @app.get("/api/manifests/{manifest_id}/piece-map", response_model=BitswarmPieceMap)
    async def piece_map(manifest_id: PathControlId) -> BitswarmPieceMap:
        if manifest_id != manifest.manifest_id:
            raise HTTPException(status_code=404, detail="manifest not found")
        return BitswarmPieceMap(manifest_id=manifest.manifest_id, piece_ids=full_piece_map(manifest))

    @app.get("/api/manifests/{manifest_id}/pieces/{piece_id}")
    async def get_piece(manifest_id: PathControlId, piece_id: PathControlId) -> Response:
        if manifest_id != manifest.manifest_id:
            raise HTTPException(status_code=404, detail="manifest not found")
        try:
            piece = piece_by_id(manifest, piece_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="piece not found") from exc
        try:
            verify_manifest_tree(resolved_root, manifest)
            _assert_root_identity(resolved_root, manifest, root_identity)
            data = read_piece(resolved_root, piece)
            verify_piece_bytes(data, piece)
            _assert_root_identity(resolved_root, manifest, root_identity)
        except OSError as exc:
            raise HTTPException(status_code=409, detail=f"piece unavailable: {piece.piece_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Bitswarm-Manifest-Id": manifest.manifest_id,
                "X-Bitswarm-Piece-Id": piece.piece_id,
                "X-Bitswarm-Piece-Sha256": piece.sha256,
            },
        )

    app.state.bitswarm_manifest = manifest
    app.state.bitswarm_root = resolved_root
    app.state.bitswarm_root_identity = root_identity
    return app


def _root_identity(root: Path, manifest: BitswarmManifest) -> tuple[int, ...]:
    if manifest.root_kind == "file":
        return file_identity_no_symlinks(root, ".", single_file=True)
    return directory_identity_no_symlinks(root, "")


def _assert_root_identity(
    root: Path,
    manifest: BitswarmManifest,
    expected: tuple[int, ...],
) -> None:
    observed = _root_identity(root, manifest)
    if observed != expected:
        raise ValueError("served root changed since seeder startup")
