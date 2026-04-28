from __future__ import annotations

import os
from pathlib import Path

import pytest

from bitswarm.protocol.errors import TreeVerificationError
from bitswarm.protocol.manifest import create_manifest
from bitswarm.protocol.verifier import verify_manifest_tree


def test_tree_verifies(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    verify_manifest_tree(sample_tree, manifest)


def test_mutated_tree_rejected(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    (sample_tree / "a.txt").write_text("changed\n", encoding="utf-8")
    with pytest.raises(TreeVerificationError):
        verify_manifest_tree(sample_tree, manifest)


def test_extra_file_rejected(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    (sample_tree / "extra.txt").write_text("extra", encoding="utf-8")
    with pytest.raises(TreeVerificationError, match="unexpected file"):
        verify_manifest_tree(sample_tree, manifest)


def test_extra_empty_directory_rejected(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    (sample_tree / "extra-dir").mkdir()
    with pytest.raises(TreeVerificationError, match="unexpected directory"):
        verify_manifest_tree(sample_tree, manifest)


def test_special_entry_rejected(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    fifo = sample_tree / "pipe"
    try:
        os.mkfifo(fifo)
    except (AttributeError, PermissionError, OSError) as exc:
        pytest.skip(f"mkfifo unavailable: {exc}")
    with pytest.raises(TreeVerificationError, match="unexpected filesystem entry"):
        verify_manifest_tree(sample_tree, manifest)


def test_empty_directory_is_preserved(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    (root / "empty").mkdir(parents=True)
    manifest = create_manifest(root, piece_size=12)
    assert [directory.path for directory in manifest.directories] == ["empty"]
    verify_manifest_tree(root, manifest)


def test_one_file_directory_manifest_rejects_bare_file(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    file_path = root / "only.txt"
    file_path.write_text("one", encoding="utf-8")
    manifest = create_manifest(root, piece_size=12)
    with pytest.raises(TreeVerificationError, match="directory-root manifest"):
        verify_manifest_tree(file_path, manifest)


def test_single_file_manifest_verifies(tmp_path: Path) -> None:
    path = tmp_path / "one.bin"
    path.write_bytes(b"single-file")
    manifest = create_manifest(path, piece_size=4)
    verify_manifest_tree(path, manifest)


def test_single_file_manifest_rejects_directory_root(tmp_path: Path) -> None:
    path = tmp_path / "one.bin"
    path.write_bytes(b"single-file")
    manifest = create_manifest(path, piece_size=4)
    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(TreeVerificationError, match="file-root manifest"):
        verify_manifest_tree(directory, manifest)


def test_verifier_rejects_file_symlink_root(tmp_path: Path) -> None:
    path = tmp_path / "one.bin"
    path.write_bytes(b"single-file")
    manifest = create_manifest(path, piece_size=4)
    link = tmp_path / "one-link.bin"
    link.symlink_to(path)
    with pytest.raises(TreeVerificationError, match="symlink roots are not supported"):
        verify_manifest_tree(link, manifest)


def test_verifier_rejects_directory_symlink_root(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    link = tmp_path / "tree-link"
    link.symlink_to(sample_tree, target_is_directory=True)
    with pytest.raises(TreeVerificationError, match="symlink roots are not supported"):
        verify_manifest_tree(link, manifest)


def test_verifier_rejects_file_under_symlinked_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    target = real_parent / "target.txt"
    target.write_text("target", encoding="utf-8")
    manifest = create_manifest(target, piece_size=4)
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(TreeVerificationError, match="symlink roots are not supported"):
        verify_manifest_tree(link_parent / "target.txt", manifest)


def test_verifier_rejects_directory_under_symlinked_parent(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    parent = sample_tree.parent
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(parent, target_is_directory=True)
    with pytest.raises(TreeVerificationError, match="symlink roots are not supported"):
        verify_manifest_tree(link_parent / sample_tree.name, manifest)


def test_verifier_rejects_relative_path_under_symlinked_cwd(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=12)
    parent = sample_tree.parent
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(parent, target_is_directory=True)
    monkeypatch.chdir(link_parent)
    monkeypatch.setenv("PWD", str(link_parent))
    with pytest.raises(TreeVerificationError, match="symlink roots are not supported"):
        verify_manifest_tree(Path(sample_tree.name), manifest)


def test_verifier_rejects_file_swapped_to_symlink_after_shape_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    target = root / "b.txt"
    target.write_text("local", encoding="utf-8")
    external = tmp_path / "external.txt"
    external.write_text("local", encoding="utf-8")
    manifest = create_manifest(root, piece_size=16)

    import bitswarm.protocol.verifier as verifier_module

    original_scan = verifier_module._scan_tree_shape
    scanned = False

    def mutating_scan(*args, **kwargs) -> None:
        nonlocal scanned
        original_scan(*args, **kwargs)
        if not scanned:
            target.unlink()
            target.symlink_to(external)
            scanned = True

    monkeypatch.setattr(verifier_module, "_scan_tree_shape", mutating_scan)
    with pytest.raises(TreeVerificationError):
        verify_manifest_tree(root, manifest)


def test_verifier_rejects_content_mutation_during_piece_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"aaaabbbb")
    manifest = create_manifest(target, piece_size=4)

    import bitswarm.protocol.pieces as pieces_module

    original_sha256_stream = pieces_module.sha256_stream
    calls = 0

    def mutating_sha256_stream(*args, **kwargs) -> str:
        nonlocal calls
        result = original_sha256_stream(*args, **kwargs)
        calls += 1
        if calls == 1:
            target.write_bytes(b"ccccbbbb")
        return result

    monkeypatch.setattr(pieces_module, "sha256_stream", mutating_sha256_stream)
    with pytest.raises(TreeVerificationError, match="changed while hashing"):
        verify_manifest_tree(target, manifest)


def test_verifier_rejects_prior_file_mutation_after_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    first = root / "a.txt"
    first.write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("bravo", encoding="utf-8")
    manifest = create_manifest(root, piece_size=4)

    import bitswarm.protocol.verifier as verifier_module

    original_hash = verifier_module.file_hashes_from_open_fd
    calls = 0

    def mutating_file_hash(*args, **kwargs):
        nonlocal calls
        result = original_hash(*args, **kwargs)
        calls += 1
        if calls == 1:
            first.write_text("omega", encoding="utf-8")
        return result

    monkeypatch.setattr(verifier_module, "file_hashes_from_open_fd", mutating_file_hash)
    with pytest.raises(TreeVerificationError, match="changed while hashing"):
        verify_manifest_tree(root, manifest)


def test_verifier_rejects_root_directory_transient_mutation_during_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("bravo", encoding="utf-8")
    manifest = create_manifest(root, piece_size=4)

    import bitswarm.protocol.verifier as verifier_module

    original_hash = verifier_module.file_hashes_from_open_fd
    calls = 0

    def mutating_file_hash(*args, **kwargs):
        nonlocal calls
        result = original_hash(*args, **kwargs)
        calls += 1
        if calls == 1:
            extra = root / "extra.txt"
            extra.write_text("transient", encoding="utf-8")
            extra.unlink()
        return result

    monkeypatch.setattr(verifier_module, "file_hashes_from_open_fd", mutating_file_hash)
    with pytest.raises(TreeVerificationError, match="root directory changed"):
        verify_manifest_tree(root, manifest)


def test_verifier_does_not_keep_all_file_descriptors_open(tmp_path: Path) -> None:
    resource = pytest.importorskip("resource")
    root = tmp_path / "tree"
    root.mkdir()
    for index in range(80):
        (root / f"f{index:03d}.txt").write_text(f"{index}\n", encoding="utf-8")
    manifest = create_manifest(root, piece_size=8)
    old_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    current_fds = len(os.listdir("/dev/fd")) if Path("/dev/fd").exists() else 16
    soft = min(old_limits[1], max(64, current_fds + 20))
    if soft <= current_fds + 5:
        pytest.skip("not enough descriptor headroom for low-fd probe")
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, old_limits[1]))
        verify_manifest_tree(root, manifest)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, old_limits)


def test_verifier_does_not_keep_directory_stack_open_for_deep_tree(tmp_path: Path) -> None:
    resource = pytest.importorskip("resource")
    root = tmp_path / "tree"
    current = root
    current.mkdir()
    for index in range(100):
        current = current / f"d{index:03d}"
        current.mkdir()
    (current / "leaf.txt").write_text("deep", encoding="utf-8")
    manifest = create_manifest(root, piece_size=8)
    old_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    current_fds = len(os.listdir("/dev/fd")) if Path("/dev/fd").exists() else 16
    soft = min(old_limits[1], max(64, current_fds + 20))
    if soft <= current_fds + 5:
        pytest.skip("not enough descriptor headroom for low-fd probe")
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, old_limits[1]))
        verify_manifest_tree(root, manifest)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, old_limits)
