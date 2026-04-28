"""Manifest creation and persistence."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from bitswarm.constants import DEFAULT_PIECE_SIZE, FILE_ROOT_PATH, MAX_PIECE_SIZE, PROTOCOL_ID

from .errors import TreeVerificationError
from .hashing import manifest_root
from .paths import resolve_root_without_symlinks
from .pieces import (
    directory_identity_no_symlinks,
    ensure_directory_no_symlinks,
    file_hashes_from_open_fd,
    file_identity_from_fd,
    file_identity_no_symlinks,
    open_file_fd_no_symlinks,
    write_file_bytes_no_symlinks,
)
from .schemas import BitswarmDirectory, BitswarmFile, BitswarmManifest, BitswarmPiece, RootKind


@dataclass(slots=True)
class _OpenManifestFile:
    relative_path: str
    identity: tuple[int, int, int, int, int]


def _iter_files(root: Path) -> list[Path]:
    if root.is_symlink():
        raise ValueError(f"symlink roots are not supported: {root}")
    if root.is_file():
        return [root]
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"symlinks are not supported in manifests: {path}")
        if path.is_file():
            files.append(path)
        elif not path.is_dir():
            raise ValueError(f"unsupported filesystem entry in manifest: {path}")
    return files


def _iter_directories(root: Path) -> list[Path]:
    if root.is_file():
        return []
    directories: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"symlinks are not supported in manifests: {path}")
        if path.is_dir():
            directories.append(path)
        elif not path.is_file():
            raise ValueError(f"unsupported filesystem entry in manifest: {path}")
    return directories


def _relative_path(path: Path, root: Path) -> str:
    if root.is_file():
        return FILE_ROOT_PATH
    return path.relative_to(root).as_posix()


def _manifest_payload_without_root(
    *,
    root_kind: RootKind,
    piece_size: int,
    total_size: int,
    directories: list[BitswarmDirectory],
    files: list[BitswarmFile],
    pieces: list[BitswarmPiece],
) -> dict[str, Any]:
    return {
        "protocol_id": PROTOCOL_ID,
        "root_kind": root_kind,
        "piece_size": piece_size,
        "total_size": total_size,
        "hash_algorithm": "sha256",
        "directories": [directory.model_dump(mode="json") for directory in directories],
        "files": [file.model_dump(mode="json") for file in files],
        "pieces": [piece.model_dump(mode="json") for piece in pieces],
    }


def create_manifest(
    path: Path,
    *,
    piece_size: int = DEFAULT_PIECE_SIZE,
    name: str | None = None,
) -> BitswarmManifest:
    """Create a deterministic manifest for a file or directory."""
    root = resolve_root_without_symlinks(path)
    if piece_size <= 0:
        raise ValueError("piece_size must be positive")
    if piece_size > MAX_PIECE_SIZE:
        raise ValueError(f"piece_size must be <= {MAX_PIECE_SIZE}")
    single_file = root.is_file()
    files: list[BitswarmFile] = []
    try:
        root_directory_identity = None if single_file else directory_identity_no_symlinks(root, "")
        directory_paths = [_relative_path(directory, root) for directory in _iter_directories(root)]
        directory_identities = {
            relative: directory_identity_no_symlinks(root, relative)
            for relative in directory_paths
        }
        for relative in directory_paths:
            ensure_directory_no_symlinks(root, relative)
    except (OSError, TreeVerificationError) as exc:
        raise ValueError(f"failed to inspect directories without following symlinks: {path}") from exc
    directories = [BitswarmDirectory(path=relative) for relative in directory_paths]
    pieces: list[BitswarmPiece] = []
    piece_index = 0
    total_size = 0
    file_identities: list[_OpenManifestFile] = []
    for file_path in _iter_files(root):
        relative = _relative_path(file_path, root)
        try:
            fd = open_file_fd_no_symlinks(root, relative, single_file=single_file)
            identity = file_identity_from_fd(fd, relative_path=relative)
        except (OSError, TreeVerificationError) as exc:
            raise ValueError(f"failed to open file without following symlinks: {relative}") from exc
        try:
            size = identity[2]
            file_pieces: list[BitswarmPiece] = []
            offset = 0
            while offset < size:
                chunk_size = min(piece_size, size - offset)
                file_pieces.append(
                    BitswarmPiece(
                        piece_id=f"p{piece_index:08d}",
                        file_path=relative,
                        offset=offset,
                        size=chunk_size,
                        sha256="0" * 64,
                    )
                )
                piece_index += 1
                offset += chunk_size
            try:
                size, file_hash, piece_hashes = file_hashes_from_open_fd(
                    fd,
                    relative_path=relative,
                    pieces=file_pieces,
                )
            except (OSError, TreeVerificationError) as exc:
                raise ValueError(f"failed to hash stable file snapshot: {relative}") from exc
        finally:
            os.close(fd)
        file_identities.append(_OpenManifestFile(relative_path=relative, identity=identity))
        hash_by_piece_id = dict(piece_hashes)
        pieces.extend(
            piece.model_copy(update={"sha256": hash_by_piece_id[piece.piece_id]})
            for piece in file_pieces
        )
        total_size += size
        files.append(BitswarmFile(path=relative, size=size, sha256=file_hash))
    _assert_manifest_snapshot_stable(
        root,
        single_file=single_file,
        root_directory_identity=root_directory_identity,
        directory_paths=directory_paths,
        directory_identities=directory_identities,
        file_identities=file_identities,
    )
    payload = _manifest_payload_without_root(
        root_kind="file" if single_file else "directory",
        piece_size=piece_size,
        total_size=total_size,
        directories=directories,
        files=files,
        pieces=pieces,
    )
    root_hash = manifest_root(payload)
    return BitswarmManifest(
        **payload,
        name=name or root.name,
        manifest_id=f"bs-{root_hash[:32]}",
        root_hash=root_hash,
    )


def manifest_payload_for_root(manifest: BitswarmManifest) -> dict[str, Any]:
    return _manifest_payload_without_root(
        root_kind=manifest.root_kind,
        piece_size=manifest.piece_size,
        total_size=manifest.total_size,
        directories=manifest.directories,
        files=manifest.files,
        pieces=manifest.pieces,
    )


def validate_manifest_root(manifest: BitswarmManifest) -> bool:
    return manifest.root_hash == manifest_root(manifest_payload_for_root(manifest))


def save_manifest(manifest: BitswarmManifest, path: Path) -> None:
    write_file_bytes_no_symlinks(
        path,
        orjson.dumps(
            manifest.model_dump(mode="json"),
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        ),
    )


def load_manifest(path: Path) -> BitswarmManifest:
    return BitswarmManifest.model_validate(orjson.loads(path.read_bytes()))


def _assert_manifest_snapshot_stable(
    root: Path,
    *,
    single_file: bool,
    root_directory_identity: tuple[int, int, int, int] | None,
    directory_paths: list[str],
    directory_identities: dict[str, tuple[int, int, int, int]],
    file_identities: list[_OpenManifestFile],
) -> None:
    try:
        if (
            root_directory_identity is not None
            and directory_identity_no_symlinks(root, "") != root_directory_identity
        ):
            raise TreeVerificationError("root directory changed while hashing")
        observed_directories = [_relative_path(directory, root) for directory in _iter_directories(root)]
        observed_files = [_relative_path(file, root) for file in _iter_files(root)]
        expected_files = [file_identity.relative_path for file_identity in file_identities]
        if observed_directories != directory_paths or observed_files != expected_files:
            raise TreeVerificationError("file tree changed while hashing")
        for relative, identity in directory_identities.items():
            if directory_identity_no_symlinks(root, relative) != identity:
                raise TreeVerificationError(f"directory changed while hashing: {relative}")
        for file_identity in file_identities:
            if (
                file_identity_no_symlinks(root, file_identity.relative_path, single_file=single_file)
                != file_identity.identity
            ):
                raise TreeVerificationError(f"file changed while hashing: {file_identity.relative_path}")
    except (OSError, TreeVerificationError) as exc:
        raise ValueError("failed to hash stable file tree snapshot") from exc
