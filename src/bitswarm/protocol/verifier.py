"""Manifest and tree verification."""

from __future__ import annotations

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .errors import PieceVerificationError, TreeVerificationError
from .hashing import sha256_bytes
from .manifest import validate_manifest_root
from .paths import resolve_root_without_symlinks
from .pieces import (
    directory_identity_no_symlinks,
    ensure_directory_no_symlinks,
    file_hashes_from_open_fd,
    file_identity_from_fd,
    file_identity_no_symlinks,
    open_directory_fd_no_symlinks,
    open_file_fd_no_symlinks,
)
from .schemas import BitswarmManifest, BitswarmPiece


@dataclass(slots=True)
class _OpenVerifiedFile:
    relative_path: str
    identity: tuple[int, int, int, int, int]


def verify_piece_bytes(data: bytes, piece: BitswarmPiece) -> None:
    if len(data) != piece.size:
        raise PieceVerificationError(
            f"piece {piece.piece_id} expected {piece.size} bytes, got {len(data)}"
        )
    observed = sha256_bytes(data)
    if observed != piece.sha256:
        raise PieceVerificationError(f"piece {piece.piece_id} hash mismatch: {observed} != {piece.sha256}")


def verify_manifest_tree(root: Path, manifest: BitswarmManifest) -> None:
    if not validate_manifest_root(manifest):
        raise TreeVerificationError("manifest root hash does not match canonical manifest payload")
    try:
        base = resolve_root_without_symlinks(root)
    except FileNotFoundError as exc:
        raise TreeVerificationError(f"missing root: {root}") from exc
    except ValueError as exc:
        raise TreeVerificationError(str(exc)) from exc
    single_file = manifest.root_kind == "file"
    if single_file and not base.is_file():
        raise TreeVerificationError("file-root manifest must be verified against a file path")
    if not single_file and not base.is_dir():
        raise TreeVerificationError("directory-root manifest must be verified against a directory path")
    expected_paths = {file.path for file in manifest.files}
    expected_directories = {directory.path for directory in manifest.directories}
    _scan_tree_shape(
        base,
        single_file=single_file,
        expected_paths=expected_paths,
        expected_directories=expected_directories,
    )
    if not single_file:
        for directory in expected_directories:
            try:
                ensure_directory_no_symlinks(base, directory)
            except OSError as exc:
                raise TreeVerificationError(f"missing directory: {directory}") from exc
    try:
        root_directory_identity = None if single_file else directory_identity_no_symlinks(base, "")
        directory_identities = {
            directory: directory_identity_no_symlinks(base, directory)
            for directory in expected_directories
        }
    except OSError as exc:
        raise TreeVerificationError("failed to inspect directory identities") from exc
    file_identities: list[_OpenVerifiedFile] = []
    for file in manifest.files:
        try:
            fd = open_file_fd_no_symlinks(base, file.path, single_file=single_file)
            identity = file_identity_from_fd(fd, relative_path=file.path)
        except OSError as exc:
            raise TreeVerificationError(f"failed to open manifest file: {file.path}") from exc
        try:
            pieces_for_file = [piece for piece in manifest.pieces if piece.file_path == file.path]
            observed_size, observed_file_hash, observed_piece_hashes = file_hashes_from_open_fd(
                fd,
                relative_path=file.path,
                pieces=pieces_for_file,
            )
        except TreeVerificationError:
            raise
        except (OSError, ValueError) as exc:
            raise TreeVerificationError(f"missing piece target: {file.path}") from exc
        finally:
            os.close(fd)
        file_identities.append(_OpenVerifiedFile(relative_path=file.path, identity=identity))
        if observed_size != file.size:
            raise TreeVerificationError(f"file size mismatch for {file.path}")
        expected_piece_hashes = [(piece.piece_id, piece.sha256) for piece in pieces_for_file]
        if observed_piece_hashes != expected_piece_hashes:
            for (piece_id, observed_piece_hash), (_expected_id, expected_piece_hash) in zip(
                observed_piece_hashes,
                expected_piece_hashes,
                strict=True,
            ):
                if observed_piece_hash == expected_piece_hash:
                    continue
                raise TreeVerificationError(
                    f"piece hash mismatch for {piece_id}: {observed_piece_hash} != {expected_piece_hash}"
                )
            raise TreeVerificationError(f"piece hash mismatch for {file.path}")
        expected_file_hash = sha256_bytes(
            b"".join(bytes.fromhex(piece.sha256) for piece in pieces_for_file)
        )
        if file.sha256 != expected_file_hash or observed_file_hash != file.sha256:
            raise TreeVerificationError(f"file piece-hash digest mismatch for {file.path}")
    _assert_verified_snapshot_stable(
        base,
        manifest=manifest,
        single_file=single_file,
        root_directory_identity=root_directory_identity,
        directory_identities=directory_identities,
        file_identities=file_identities,
    )


def _scan_tree_shape(
    base: Path,
    *,
    single_file: bool,
    expected_paths: set[str],
    expected_directories: set[str],
) -> None:
    if single_file:
        if base.is_symlink() or not base.is_file():
            raise TreeVerificationError("file-root manifest must be verified against a regular file path")
        return
    pending_directories = [""]
    while pending_directories:
        prefix = pending_directories.pop()
        pending_directories.extend(
            _scan_directory_path(
                base,
                prefix=prefix,
                expected_paths=expected_paths,
                expected_directories=expected_directories,
            )
        )


def _scan_directory_path(
    base: Path,
    *,
    prefix: str,
    expected_paths: set[str],
    expected_directories: set[str],
) -> list[str]:
    directory_path = base if not prefix else base.joinpath(*prefix.split("/"))
    try:
        directory_fd = open_directory_fd_no_symlinks(directory_path)
    except OSError as exc:
        raise TreeVerificationError(f"missing directory: {prefix or '.'}") from exc
    try:
        return _scan_directory_fd(
            directory_fd,
            prefix=prefix,
            expected_paths=expected_paths,
            expected_directories=expected_directories,
        )
    finally:
        os.close(directory_fd)


def _scan_directory_fd(
    directory_fd: int,
    *,
    prefix: str,
    expected_paths: set[str],
    expected_directories: set[str],
) -> list[str]:
    child_directories: list[str] = []
    for name in sorted(os.listdir(directory_fd)):
        relative = f"{prefix}/{name}" if prefix else name
        try:
            child_fd = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
        except OSError as directory_error:
            if directory_error.errno == errno.ELOOP:
                raise TreeVerificationError(f"unexpected symlink in tree: {relative}") from directory_error
            try:
                file_fd = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory_fd,
                )
            except OSError as file_error:
                if file_error.errno == errno.ELOOP:
                    raise TreeVerificationError(f"unexpected symlink in tree: {relative}") from file_error
                raise TreeVerificationError(f"unexpected filesystem entry: {relative}") from file_error
            try:
                observed = os.fstat(file_fd)
                if not stat.S_ISREG(observed.st_mode):
                    raise TreeVerificationError(f"unexpected filesystem entry: {relative}")
                if observed.st_nlink != 1:
                    raise TreeVerificationError(f"hard-linked files are not supported: {relative}")
                if relative not in expected_paths:
                    raise TreeVerificationError(f"unexpected file: {relative}")
            finally:
                os.close(file_fd)
            continue
        try:
            observed = os.fstat(child_fd)
            if not stat.S_ISDIR(observed.st_mode):
                raise TreeVerificationError(f"unexpected filesystem entry: {relative}")
            if relative not in expected_directories:
                raise TreeVerificationError(f"unexpected directory: {relative}")
            child_directories.append(relative)
        finally:
            os.close(child_fd)
    return child_directories


def _assert_verified_snapshot_stable(
    base: Path,
    *,
    manifest: BitswarmManifest,
    single_file: bool,
    root_directory_identity: tuple[int, int, int, int] | None,
    directory_identities: dict[str, tuple[int, int, int, int]],
    file_identities: list[_OpenVerifiedFile],
) -> None:
    expected_paths = {file.path for file in manifest.files}
    expected_directories = {directory.path for directory in manifest.directories}
    _scan_tree_shape(
        base,
        single_file=single_file,
        expected_paths=expected_paths,
        expected_directories=expected_directories,
    )
    try:
        if (
            root_directory_identity is not None
            and directory_identity_no_symlinks(base, "") != root_directory_identity
        ):
            raise TreeVerificationError("root directory changed while hashing")
        for directory, identity in directory_identities.items():
            if directory_identity_no_symlinks(base, directory) != identity:
                raise TreeVerificationError(f"directory changed while hashing: {directory}")
        for file_identity in file_identities:
            if (
                file_identity_no_symlinks(base, file_identity.relative_path, single_file=single_file)
                != file_identity.identity
            ):
                raise TreeVerificationError(f"file changed while hashing: {file_identity.relative_path}")
    except OSError as exc:
        raise TreeVerificationError("tree changed while hashing") from exc
