"""Piece read/write helpers."""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from bitswarm.constants import FILE_ROOT_PATH

from .errors import CachePromotionError, TreeVerificationError
from .hashing import sha256_bytes, sha256_stream
from .schemas import BitswarmManifest, BitswarmPiece


def piece_by_id(manifest: BitswarmManifest, piece_id: str) -> BitswarmPiece:
    for piece in manifest.pieces:
        if piece.piece_id == piece_id:
            return piece
    raise KeyError(f"unknown piece id: {piece_id}")


def read_piece(root: Path, piece: BitswarmPiece) -> bytes:
    try:
        fd = open_file_fd_no_symlinks(root, piece.file_path, single_file=piece.file_path == FILE_ROOT_PATH)
    except OSError as exc:
        raise TreeVerificationError(
            f"failed to open piece target without following symlinks: {piece.file_path}"
        ) from exc
    with os.fdopen(fd, "rb") as file:
        _ensure_regular_unaliased_fd(file.fileno(), piece.file_path)
        file.seek(piece.offset)
        data = file.read(piece.size)
    if len(data) != piece.size:
        raise ValueError(f"piece {piece.piece_id} expected {piece.size} bytes, got {len(data)}")
    return data


def verified_piece_bytes(data: bytes, piece: BitswarmPiece) -> bytes:
    if len(data) != piece.size:
        raise ValueError(f"piece {piece.piece_id} expected {piece.size} bytes, got {len(data)}")
    if sha256_bytes(data) != piece.sha256:
        raise ValueError(f"piece {piece.piece_id} hash mismatch")
    return data


def file_hashes_from_open_fd(
    fd: int,
    *,
    relative_path: str,
    pieces: list[BitswarmPiece],
) -> tuple[int, str, list[tuple[str, str]]]:
    observed = os.fstat(fd)
    _ensure_regular_unaliased_stat(observed, relative_path)
    size = observed.st_size
    piece_hashes: list[tuple[str, str]] = []
    with os.fdopen(os.dup(fd), "rb") as file:
        for piece in pieces:
            if piece.file_path != relative_path:
                raise TreeVerificationError(f"piece {piece.piece_id} does not belong to {relative_path}")
            if piece.offset < 0 or piece.size < 0 or piece.offset + piece.size > size:
                raise TreeVerificationError(f"piece {piece.piece_id} is outside file bounds: {relative_path}")
            file.seek(piece.offset)
            piece_hash = sha256_stream(file, size=piece.size)
            piece_hashes.append((piece.piece_id, piece_hash))
    after = os.fstat(fd)
    _ensure_regular_unaliased_stat(after, relative_path)
    if (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ) != (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    ):
        raise TreeVerificationError(f"file changed while hashing: {relative_path}")
    file_hash = sha256_bytes(b"".join(bytes.fromhex(piece_hash) for _piece_id, piece_hash in piece_hashes))
    return size, file_hash, piece_hashes


def file_identity_from_fd(fd: int, *, relative_path: str) -> tuple[int, int, int, int, int]:
    observed = os.fstat(fd)
    _ensure_regular_unaliased_stat(observed, relative_path)
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def write_piece(root: Path, piece: BitswarmPiece, data: bytes, *, single_file: bool = False) -> None:
    verified_piece_bytes(data, piece)
    flags = os.O_CREAT | os.O_RDWR
    try:
        fd = open_file_fd_no_symlinks(
            root,
            piece.file_path,
            single_file=single_file,
            flags=flags,
            mode=0o644,
            create_parents=not single_file,
        )
    except OSError as exc:
        raise CachePromotionError(
            f"failed to open piece target without following symlinks: {piece.file_path}"
        ) from exc
    with os.fdopen(fd, "r+b") as file:
        _ensure_regular_unaliased_fd(file.fileno(), piece.file_path)
        file.seek(piece.offset)
        file.write(data)


def open_file_fd_no_symlinks(
    root: Path,
    relative_path: str,
    *,
    single_file: bool = False,
    flags: int = os.O_RDONLY,
    mode: int = 0o644,
    create_parents: bool = False,
) -> int:
    """Open a file without following symlinks in any traversed component."""
    nofollow_flags = flags | _nofollow_flag()
    if single_file:
        path = root.expanduser()
        parent_fd = _open_dir_fd_no_symlinks(path.parent)
        try:
            return os.open(path.name, nofollow_flags, mode, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)

    parent_fd, leaf = _open_parent_fd_no_symlinks(
        root.expanduser(),
        relative_path,
        create_parents=create_parents,
    )
    try:
        return os.open(leaf, nofollow_flags, mode, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def file_stats_and_hash_no_symlinks(
    root: Path,
    relative_path: str,
    *,
    single_file: bool = False,
) -> tuple[int, str]:
    fd = open_file_fd_no_symlinks(root, relative_path, single_file=single_file)
    with os.fdopen(fd, "rb") as file:
        observed = os.fstat(file.fileno())
        _ensure_regular_unaliased_stat(observed, relative_path)
        return observed.st_size, sha256_stream(file)


def file_range_hash_no_symlinks(
    root: Path,
    relative_path: str,
    *,
    offset: int,
    size: int,
    single_file: bool = False,
) -> str:
    fd = open_file_fd_no_symlinks(root, relative_path, single_file=single_file)
    with os.fdopen(fd, "rb") as file:
        observed = os.fstat(file.fileno())
        _ensure_regular_unaliased_stat(observed, relative_path)
        file.seek(offset)
        return sha256_stream(file, size=size)


def file_identity_no_symlinks(
    root: Path,
    relative_path: str,
    *,
    single_file: bool = False,
) -> tuple[int, int, int, int, int]:
    fd = open_file_fd_no_symlinks(root, relative_path, single_file=single_file)
    with os.fdopen(fd, "rb") as file:
        observed = os.fstat(file.fileno())
        _ensure_regular_unaliased_stat(observed, relative_path)
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )


def directory_identity_no_symlinks(root: Path, relative_path: str) -> tuple[int, int, int, int]:
    fd = _open_directory_path_no_symlinks(root.expanduser(), _relative_parts(relative_path))
    try:
        observed = os.fstat(fd)
        if not stat.S_ISDIR(observed.st_mode):
            raise TreeVerificationError(f"piece target is not a directory: {relative_path}")
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )
    finally:
        os.close(fd)


def ensure_directory_no_symlinks(root: Path, relative_path: str) -> None:
    fd = _open_directory_path_no_symlinks(root.expanduser(), _relative_parts(relative_path))
    os.close(fd)


def make_directory_no_symlinks(root: Path, relative_path: str) -> None:
    parts = _relative_parts(relative_path)
    if not parts:
        return
    parent_fd, leaf = _open_parent_fd_from_parts(root.expanduser(), parts, create_parents=True)
    try:
        try:
            os.mkdir(leaf, 0o755, dir_fd=parent_fd)
        except FileExistsError:
            child_fd = _open_child_dir_fd_no_symlinks(parent_fd, leaf)
            os.close(child_fd)
    except OSError as exc:
        raise CachePromotionError(
            f"failed to create directory without following symlinks: {relative_path}"
        ) from exc
    finally:
        os.close(parent_fd)


def create_empty_file_no_symlinks(root: Path, relative_path: str, *, single_file: bool = False) -> None:
    try:
        fd = open_file_fd_no_symlinks(
            root,
            relative_path,
            single_file=single_file,
            flags=os.O_CREAT | os.O_RDWR,
            mode=0o644,
            create_parents=not single_file,
        )
    except OSError as exc:
        raise CachePromotionError(
            f"failed to create empty file without following symlinks: {relative_path}"
        ) from exc
    with os.fdopen(fd, "r+b") as file:
        _ensure_regular_unaliased_fd(file.fileno(), relative_path)
        file.truncate(0)


def _open_parent_fd_no_symlinks(
    root: Path,
    relative_path: str,
    *,
    create_parents: bool,
) -> tuple[int, str]:
    parts = _relative_parts(relative_path)
    if not parts:
        raise ValueError("relative file path must not be empty")
    return _open_parent_fd_from_parts(root, parts, create_parents=create_parents)


def _open_parent_fd_from_parts(root: Path, parts: list[str], *, create_parents: bool) -> tuple[int, str]:
    if not parts:
        raise ValueError("relative file path must not be empty")
    fd = _open_dir_fd_no_symlinks(root)
    try:
        for part in parts[:-1]:
            if create_parents:
                with suppress(FileExistsError):
                    os.mkdir(part, 0o755, dir_fd=fd)
            child_fd = _open_child_dir_fd_no_symlinks(fd, part)
            os.close(fd)
            fd = child_fd
    except Exception:
        os.close(fd)
        raise
    return fd, parts[-1]


def _open_directory_path_no_symlinks(root: Path, parts: list[str]) -> int:
    fd = _open_dir_fd_no_symlinks(root)
    try:
        for part in parts:
            child_fd = _open_child_dir_fd_no_symlinks(fd, part)
            os.close(fd)
            fd = child_fd
    except Exception:
        os.close(fd)
        raise
    return fd


def _open_dir_fd_no_symlinks(path: Path) -> int:
    return open_directory_fd_no_symlinks(path)


def open_directory_fd_no_symlinks(path: Path) -> int:
    path = _absolute_path_for_fd_traversal(path)
    fd = os.open(path.anchor, os.O_RDONLY | _directory_flag())
    try:
        for part in _absolute_parts(path):
            child_fd = _open_child_dir_fd_no_symlinks(fd, part)
            os.close(fd)
            fd = child_fd
    except Exception:
        os.close(fd)
        raise
    observed = os.fstat(fd)
    if not stat.S_ISDIR(observed.st_mode):
        os.close(fd)
        raise NotADirectoryError(path)
    return fd


def open_parent_fd_for_path_no_symlinks(path: Path, *, create_parents: bool = False) -> tuple[int, str]:
    path = _absolute_path_for_fd_traversal(path)
    parts = _absolute_parts(path)
    if not parts:
        raise ValueError("path must include a leaf")
    fd = os.open(path.anchor, os.O_RDONLY | _directory_flag())
    try:
        for part in parts[:-1]:
            if create_parents:
                with suppress(FileExistsError):
                    os.mkdir(part, 0o755, dir_fd=fd)
            child_fd = _open_child_dir_fd_no_symlinks(fd, part)
            os.close(fd)
            fd = child_fd
    except Exception:
        os.close(fd)
        raise
    return fd, parts[-1]


def make_absolute_directory_tree_no_symlinks(path: Path) -> None:
    path = _absolute_path_for_fd_traversal(path)
    fd = os.open(path.anchor, os.O_RDONLY | _directory_flag())
    try:
        for part in _absolute_parts(path):
            with suppress(FileExistsError):
                os.mkdir(part, 0o755, dir_fd=fd)
            child_fd = _open_child_dir_fd_no_symlinks(fd, part)
            os.close(fd)
            fd = child_fd
    finally:
        os.close(fd)


def make_absolute_directory_no_symlinks(path: Path) -> None:
    parent_fd, leaf = open_parent_fd_for_path_no_symlinks(path)
    try:
        with suppress(FileExistsError):
            os.mkdir(leaf, 0o755, dir_fd=parent_fd)
        child_fd = _open_child_dir_fd_no_symlinks(parent_fd, leaf)
        os.close(child_fd)
    finally:
        os.close(parent_fd)


def create_regular_file_no_symlinks(path: Path) -> None:
    parent_fd, leaf = open_parent_fd_for_path_no_symlinks(path)
    try:
        fd = os.open(leaf, os.O_CREAT | os.O_EXCL | os.O_RDWR | _nofollow_flag(), 0o644, dir_fd=parent_fd)
    except FileExistsError as exc:
        raise CachePromotionError(f"file path already exists: {path}") from exc
    except OSError as exc:
        raise CachePromotionError(f"failed to create file without following symlinks: {path}") from exc
    finally:
        os.close(parent_fd)
    try:
        _ensure_regular_unaliased_fd(fd, str(path))
    finally:
        os.close(fd)


def write_file_bytes_no_symlinks(path: Path, data: bytes) -> None:
    parent_fd, leaf = open_parent_fd_for_path_no_symlinks(path, create_parents=True)
    temp_name = f".{leaf}.{uuid4().hex}.tmp"
    temp_fd: int | None = None
    try:
        if _leaf_is_symlink(parent_fd, leaf):
            raise ValueError(f"output path must not be a symlink: {path}")
        temp_fd = os.open(
            temp_name,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | _nofollow_flag(),
            0o644,
            dir_fd=parent_fd,
        )
        with os.fdopen(temp_fd, "wb") as file:
            temp_fd = None
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        if _leaf_is_symlink(parent_fd, leaf):
            raise ValueError(f"output path must not be a symlink: {path}")
        os.replace(temp_name, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    except Exception:
        with suppress(FileNotFoundError):
            os.unlink(temp_name, dir_fd=parent_fd)
        raise
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        os.close(parent_fd)


def _open_child_dir_fd_no_symlinks(parent_fd: int, name: str) -> int:
    flags = os.O_RDONLY | _directory_flag() | _nofollow_flag()
    fd = os.open(name, flags, dir_fd=parent_fd)
    observed = os.fstat(fd)
    if not stat.S_ISDIR(observed.st_mode):
        os.close(fd)
        raise NotADirectoryError(name)
    return fd


def _leaf_is_symlink(parent_fd: int, name: str) -> bool:
    try:
        observed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(observed.st_mode)


def _relative_parts(relative_path: str) -> list[str]:
    if relative_path in {"", FILE_ROOT_PATH}:
        return []
    parts = relative_path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"invalid relative path: {relative_path}")
    return parts


def _absolute_path_for_fd_traversal(path: Path) -> Path:
    from .paths import _physical_path, is_top_level_compatibility_symlink

    absolute = _physical_path(path.expanduser())
    if not absolute.is_absolute():
        absolute = Path.cwd() / absolute
    parts = absolute.parts
    if len(parts) > 1:
        first = Path(absolute.anchor) / parts[1]
        if is_top_level_compatibility_symlink(first):
            absolute = first.resolve(strict=True).joinpath(*parts[2:])
    return absolute


def _absolute_parts(path: Path) -> list[str]:
    parts = list(path.parts)
    if not path.is_absolute() or not parts:
        raise ValueError(f"path must be absolute: {path}")
    cleaned = parts[1:]
    if any(part in {"", ".", ".."} for part in cleaned):
        raise ValueError(f"invalid path component: {path}")
    return cleaned


def _ensure_regular_unaliased_fd(fd: int, relative_path: str) -> None:
    _ensure_regular_unaliased_stat(os.fstat(fd), relative_path)


def _ensure_regular_unaliased_stat(observed: os.stat_result, relative_path: str) -> None:
    if not stat.S_ISREG(observed.st_mode):
        raise TreeVerificationError(f"piece target is not a regular file: {relative_path}")
    if observed.st_nlink != 1:
        raise TreeVerificationError(f"hard-linked files are not supported: {relative_path}")


def _nofollow_flag() -> int:
    return getattr(os, "O_NOFOLLOW", 0)


def _directory_flag() -> int:
    return getattr(os, "O_DIRECTORY", 0)
