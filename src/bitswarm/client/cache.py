"""Cache and promotion helpers."""

from __future__ import annotations

import os
import shutil
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from bitswarm.protocol.errors import CachePromotionError, TreeVerificationError
from bitswarm.protocol.paths import resolve_target_without_symlink_ancestors
from bitswarm.protocol.pieces import (
    create_empty_file_no_symlinks,
    create_regular_file_no_symlinks,
    directory_identity_no_symlinks,
    file_identity_no_symlinks,
    make_absolute_directory_no_symlinks,
    make_absolute_directory_tree_no_symlinks,
    make_directory_no_symlinks,
    open_directory_fd_no_symlinks,
    read_piece,
    write_piece,
)
from bitswarm.protocol.schemas import BitswarmManifest
from bitswarm.protocol.verifier import verify_manifest_tree


@dataclass(frozen=True, slots=True)
class StagingGuard:
    path: Path
    is_file: bool
    st_dev: int
    st_ino: int


@dataclass(frozen=True, slots=True)
class TreeFingerprint:
    root: tuple[int, int, int, int]
    directories: tuple[tuple[str, tuple[int, int, int, int]], ...]
    files: tuple[tuple[str, tuple[int, int, int, int, int]], ...]


def staging_path(output_path: Path, manifest: BitswarmManifest) -> Path:
    return output_path.with_name(f".{output_path.name}.{manifest.manifest_id}.{uuid4().hex}.partial")


def prepare_staging(destination: Path, manifest: BitswarmManifest) -> StagingGuard:
    destination = resolve_target_without_symlink_ancestors(destination)
    _validate_destination_shape(destination, manifest)
    staging = staging_path(destination, manifest)
    if staging.exists() or staging.is_symlink():
        raise CachePromotionError(f"staging path already exists: {staging}")
    _safe_create_parent(staging.parent)
    try:
        staging = resolve_target_without_symlink_ancestors(staging)
    except ValueError as exc:
        raise CachePromotionError(f"staging path became unsafe: {staging}") from exc
    if staging.exists() or staging.is_symlink():
        raise CachePromotionError(f"staging path already exists: {staging}")
    is_file = manifest.root_kind == "file"
    if is_file:
        _safe_create_file(staging)
    else:
        _safe_create_directory(staging)
    observed = staging.stat(follow_symlinks=False)
    return StagingGuard(
        path=staging,
        is_file=is_file,
        st_dev=observed.st_dev,
        st_ino=observed.st_ino,
    )


def ensure_staging_guard(guard: StagingGuard) -> None:
    if guard.path.is_symlink() or not guard.path.exists():
        raise CachePromotionError(f"staging path changed unexpectedly: {guard.path}")
    observed = guard.path.stat(follow_symlinks=False)
    mode_matches = stat.S_ISREG(observed.st_mode) if guard.is_file else stat.S_ISDIR(observed.st_mode)
    if not mode_matches or observed.st_dev != guard.st_dev or observed.st_ino != guard.st_ino:
        raise CachePromotionError(f"staging path changed unexpectedly: {guard.path}")


def promote_verified_tree(
    staging: Path,
    destination: Path,
    manifest: BitswarmManifest,
    *,
    guard: StagingGuard | None = None,
) -> None:
    """Promote staging to destination only after full verification."""
    destination = resolve_target_without_symlink_ancestors(destination)
    if guard is not None:
        ensure_staging_guard(guard)
    verify_manifest_tree(staging, manifest)
    if guard is not None:
        ensure_staging_guard(guard)
    _validate_destination_shape(destination, manifest)
    promotion = destination.with_name(f".{destination.name}.{manifest.manifest_id}.{uuid4().hex}.promote")
    backup = destination.with_name(f".{destination.name}.{uuid4().hex}.old")
    backed_up = False
    installed = False
    try:
        _copy_verified_tree(staging, promotion, manifest)
        fingerprint = _fingerprint_verified_tree(promotion, manifest)
        if destination.exists():
            _replace_path_no_symlinks(destination, backup)
            backed_up = True
        if guard is not None:
            ensure_staging_guard(guard)
        _ensure_tree_fingerprint(promotion, manifest, fingerprint)
        _replace_verified_path_no_symlinks(promotion, destination, manifest, fingerprint)
        installed = True
        try:
            verify_manifest_tree(destination, manifest)
        except TreeVerificationError as exc:
            raise CachePromotionError(f"promoted tree failed verification: {destination}") from exc
    except OSError as exc:
        _restore_backup(destination, backup, backed_up=backed_up)
        raise CachePromotionError(f"failed to promote verified tree to {destination}") from exc
    except (CachePromotionError, TreeVerificationError):
        if installed and (destination.exists() or destination.is_symlink()):
            clear_staging(destination)
        _restore_backup(destination, backup, backed_up=backed_up)
        clear_staging(promotion)
        raise
    if backup.exists():
        if backup.is_dir():
            shutil.rmtree(backup)
        else:
            backup.unlink()
    clear_staging(staging)


def clear_staging(staging: Path) -> None:
    if staging.exists() or staging.is_symlink():
        if staging.is_symlink():
            staging.unlink()
        elif staging.is_dir():
            shutil.rmtree(staging)
        else:
            staging.unlink()


def _restore_backup(destination: Path, backup: Path, *, backed_up: bool) -> None:
    if not backed_up or not backup.exists():
        return
    last_error: Exception | None = None
    for _attempt in range(8):
        try:
            if destination.exists() or destination.is_symlink():
                clear_staging(destination)
            _replace_path_no_symlinks(backup, destination)
            return
        except (OSError, ValueError) as exc:
            last_error = exc
            if destination.exists() or destination.is_symlink():
                clear_staging(destination)
            if not backup.exists():
                break
    message = f"failed to restore backup after promotion failure: {destination}"
    raise CachePromotionError(message) from last_error


def _validate_destination_shape(destination: Path, manifest: BitswarmManifest) -> None:
    if destination.is_symlink():
        raise CachePromotionError(f"destination must not be a symlink: {destination}")
    if not destination.exists():
        return
    if manifest.root_kind == "file" and not destination.is_file():
        raise CachePromotionError(f"file-root manifest cannot replace non-file destination: {destination}")
    if manifest.root_kind == "directory" and not destination.is_dir():
        raise CachePromotionError(
            f"directory-root manifest cannot replace non-directory destination: {destination}"
        )


def _safe_create_parent(parent: Path) -> None:
    try:
        make_absolute_directory_tree_no_symlinks(parent)
    except (OSError, ValueError) as exc:
        raise CachePromotionError(f"failed to create parent without following symlinks: {parent}") from exc


def _safe_create_directory(path: Path) -> None:
    try:
        make_absolute_directory_no_symlinks(path)
    except (OSError, ValueError) as exc:
        raise CachePromotionError(f"failed to create directory safely: {path}") from exc


def _safe_create_file(path: Path) -> None:
    try:
        create_regular_file_no_symlinks(path)
    except (OSError, ValueError) as exc:
        raise CachePromotionError(f"failed to create file safely: {path}") from exc


def _copy_verified_tree(source: Path, destination: Path, manifest: BitswarmManifest) -> None:
    if destination.exists() or destination.is_symlink():
        raise CachePromotionError(f"promotion path already exists: {destination}")
    _safe_create_parent(destination.parent)
    if manifest.root_kind == "file":
        _safe_create_file(destination)
        single_file = True
    else:
        _safe_create_directory(destination)
        single_file = False
        for directory in manifest.directories:
            make_directory_no_symlinks(destination, directory.path)
    try:
        for file in manifest.files:
            if file.size == 0:
                create_empty_file_no_symlinks(destination, file.path, single_file=single_file)
        for piece in manifest.pieces:
            data = read_piece(source, piece)
            write_piece(destination, piece, data, single_file=single_file)
    except (OSError, ValueError, TreeVerificationError) as exc:
        clear_staging(destination)
        raise CachePromotionError(f"failed to copy verified staging tree: {source}") from exc


def _replace_path_no_symlinks(source: Path, destination: Path) -> None:
    source = resolve_target_without_symlink_ancestors(source)
    destination = resolve_target_without_symlink_ancestors(destination)
    source_parent_fd = open_directory_fd_no_symlinks(source.parent)
    destination_parent_fd = open_directory_fd_no_symlinks(destination.parent)
    try:
        os.replace(
            source.name,
            destination.name,
            src_dir_fd=source_parent_fd,
            dst_dir_fd=destination_parent_fd,
        )
    finally:
        os.close(source_parent_fd)
        os.close(destination_parent_fd)


def _replace_verified_path_no_symlinks(
    source: Path,
    destination: Path,
    manifest: BitswarmManifest,
    fingerprint: TreeFingerprint,
) -> None:
    try:
        source = resolve_target_without_symlink_ancestors(source)
        destination = resolve_target_without_symlink_ancestors(destination)
    except ValueError as exc:
        raise CachePromotionError(f"verified install path became unsafe: {source} -> {destination}") from exc
    source_parent_fd = open_directory_fd_no_symlinks(source.parent)
    destination_parent_fd = open_directory_fd_no_symlinks(destination.parent)
    source_fd: int | None = None
    destination_fd: int | None = None
    try:
        source_fd = os.open(
            source.name,
            os.O_RDONLY | (os.O_DIRECTORY if manifest.root_kind == "directory" else 0) | _nofollow_flag(),
            dir_fd=source_parent_fd,
        )
        source_stat = os.fstat(source_fd)
        _ensure_tree_fingerprint(source, manifest, fingerprint)
        if (fingerprint.root[0], fingerprint.root[1]) != (source_stat.st_dev, source_stat.st_ino):
            raise CachePromotionError(f"verified source changed before install: {source}")
        os.replace(
            source.name,
            destination.name,
            src_dir_fd=source_parent_fd,
            dst_dir_fd=destination_parent_fd,
        )
        destination_fd = os.open(
            destination.name,
            os.O_RDONLY | (os.O_DIRECTORY if manifest.root_kind == "directory" else 0) | _nofollow_flag(),
            dir_fd=destination_parent_fd,
        )
        destination_stat = os.fstat(destination_fd)
        if (destination_stat.st_dev, destination_stat.st_ino) != (source_stat.st_dev, source_stat.st_ino):
            clear_staging(destination)
            raise CachePromotionError(f"verified source changed during install: {source}")
    finally:
        if source_fd is not None:
            os.close(source_fd)
        if destination_fd is not None:
            os.close(destination_fd)
        os.close(source_parent_fd)
        os.close(destination_parent_fd)


def _fingerprint_verified_tree(root: Path, manifest: BitswarmManifest) -> TreeFingerprint:
    verify_manifest_tree(root, manifest)
    single_file = manifest.root_kind == "file"
    if single_file:
        root_stat = root.stat(follow_symlinks=False)
        root_identity = (
            root_stat.st_dev,
            root_stat.st_ino,
            root_stat.st_mtime_ns,
            root_stat.st_ctime_ns,
        )
        directory_identities: tuple[tuple[str, tuple[int, int, int, int]], ...] = ()
    else:
        root_identity = directory_identity_no_symlinks(root, "")
        directory_identities = tuple(
            (directory.path, directory_identity_no_symlinks(root, directory.path))
            for directory in manifest.directories
        )
    file_identities = tuple(
        (
            file.path,
            file_identity_no_symlinks(root, file.path, single_file=single_file),
        )
        for file in manifest.files
    )
    return TreeFingerprint(
        root=root_identity,
        directories=directory_identities,
        files=file_identities,
    )


def _ensure_tree_fingerprint(
    root: Path,
    manifest: BitswarmManifest,
    fingerprint: TreeFingerprint,
) -> None:
    try:
        observed = _fingerprint_verified_tree(root, manifest)
    except TreeVerificationError as exc:
        raise CachePromotionError(f"verified tree changed before promotion: {root}") from exc
    if observed != fingerprint:
        raise CachePromotionError(f"verified tree changed before promotion: {root}")


def _nofollow_flag() -> int:
    return getattr(os, "O_NOFOLLOW", 0)
