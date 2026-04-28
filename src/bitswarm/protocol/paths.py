"""Path safety helpers."""

from __future__ import annotations

import os
from pathlib import Path


def _physical_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    pwd = os.environ.get("PWD")
    cwd = Path.cwd()
    if pwd:
        pwd_path = Path(pwd)
        try:
            if pwd_path.exists() and pwd_path.resolve() == cwd:
                return pwd_path / path
        except OSError:
            pass
    return cwd / path


def resolve_root_without_symlinks(path: Path) -> Path:
    """Resolve a user root only after rejecting symlinks in the supplied path."""
    expanded = _physical_path(path.expanduser())
    if not expanded.exists():
        raise FileNotFoundError(expanded)
    for candidate in (expanded, *expanded.parents):
        if candidate.exists() and candidate.is_symlink():
            if candidate != expanded and candidate.parent == Path(candidate.anchor):
                # macOS commonly exposes /var and /tmp as top-level compatibility
                # symlinks into /private. Reject user-controlled symlink
                # components, but do not make ordinary temp/cache roots unusable.
                continue
            raise ValueError(f"symlink roots are not supported: {path}")
    return expanded.resolve()


def is_top_level_compatibility_symlink(path: Path) -> bool:
    """Return true for macOS top-level compatibility symlinks such as /tmp."""
    return path.parent == Path(path.anchor) and path.is_symlink()


def resolve_target_without_symlink_ancestors(path: Path) -> Path:
    """Resolve a target path only after rejecting existing symlink components."""
    expanded = _physical_path(path.expanduser())
    _reject_target_symlink_components(expanded, original=path)
    parent = expanded.parent
    if parent.exists():
        return parent.resolve() / expanded.name
    return expanded


def _reject_target_symlink_components(expanded: Path, *, original: Path) -> None:
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    current = Path(expanded.anchor)
    for part in expanded.parts[1:]:
        current = current / part
        if current.is_symlink():
            if is_top_level_compatibility_symlink(current):
                continue
            if current == expanded:
                raise ValueError(f"output path must not be a symlink: {original}")
            raise ValueError(f"output path must not include symlink ancestors: {original}")
        if not current.exists():
            break
