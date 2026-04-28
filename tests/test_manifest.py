from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from bitswarm.constants import MAX_PIECE_SIZE
from bitswarm.protocol.hashing import manifest_root
from bitswarm.protocol.manifest import create_manifest, load_manifest, save_manifest, validate_manifest_root
from bitswarm.protocol.schemas import BitswarmManifest


def _refresh_manifest_identity(payload: dict) -> None:
    canonical_payload = {
        key: value
        for key, value in payload.items()
        if key not in {"manifest_id", "root_hash", "name"}
    }
    root_hash = manifest_root(canonical_payload)
    payload["root_hash"] = root_hash
    payload["manifest_id"] = f"bs-{root_hash[:32]}"


def test_manifest_is_deterministic(sample_tree: Path) -> None:
    first = create_manifest(sample_tree, piece_size=8)
    second = create_manifest(sample_tree, piece_size=8)
    assert first.model_dump(mode="json") == second.model_dump(mode="json")
    assert first.manifest_id.startswith("bs-")
    assert validate_manifest_root(first)


def test_manifest_round_trips(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    path = tmp_path / "manifest.json"
    save_manifest(manifest, path)
    assert load_manifest(path) == manifest


def test_save_manifest_rejects_symlink_output(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    target = tmp_path / "target.json"
    target.write_text("keep me", encoding="utf-8")
    link = tmp_path / "manifest.json"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="output path must not be a symlink"):
        save_manifest(manifest, link)
    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == "keep me"


def test_public_schema_rejects_unknown_fields(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["unexpected"] = True
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_parent_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "../escape.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_dot_segment_alias_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = f"./{payload['files'][0]['path']}"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_repeated_separator_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = f"nested//{payload['files'][0]['path']}"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_backslash_alias_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "nested\\a.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_windows_absolute_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "C:/escape.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_colon_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "safe:colon.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_windows_drive_relative_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "C:escape.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_escaping_piece_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["pieces"][0]["file_path"] = "../escape.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_windows_piece_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["pieces"][0]["file_path"] = "C:/escape.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_windows_directory_paths(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["directories"][0]["path"] = "C:/escape"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_piece_for_undeclared_file(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["pieces"][0]["file_path"] = "missing.txt"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_tampered_protocol_id(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["protocol_id"] = "bitswarm-9.9"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_tampered_manifest_id(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["manifest_id"] = "alias"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_tampered_root_hash_even_with_matching_manifest_id(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["name"] = "forged-name"
    payload["root_hash"] = "1" * 64
    payload["manifest_id"] = f"bs-{payload['root_hash'][:32]}"
    with pytest.raises(ValidationError, match="root_hash must match"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_tampered_total_size(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["total_size"] += 1
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_file_hash_inconsistent_with_piece_hashes(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["sha256"] = "0" * 64
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="file sha256"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_piece_gap(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=8)
    payload = manifest.model_dump(mode="json")
    target_file = payload["pieces"][0]["file_path"]
    payload["pieces"] = [
        piece
        for piece in payload["pieces"]
        if not (piece["file_path"] == target_file and piece["offset"] == 0)
    ]
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_piece_overlap(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=8)
    payload = manifest.model_dump(mode="json")
    if len(payload["pieces"]) < 2:
        pytest.skip("fixture needs at least two pieces")
    payload["pieces"][1]["file_path"] = payload["pieces"][0]["file_path"]
    payload["pieces"][1]["offset"] = 1
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_noncanonical_piece_size_when_rehashed(tmp_path: Path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"abcdefgh")
    manifest = create_manifest(path, piece_size=4)
    payload = manifest.model_dump(mode="json")
    payload["pieces"] = [
        {
            "piece_id": "p00000000",
            "file_path": ".",
            "offset": 0,
            "size": 2,
            "sha256": "fb8e20fc2e4c3f248c60c39bd652f3c1347298bb977b8b4d5903b85055620603",
        },
        {
            "piece_id": "p00000001",
            "file_path": ".",
            "offset": 2,
            "size": 2,
            "sha256": "21e721c35a5823fdb452fa2f9f0a612c74fb952e06927489c6b27a43b817bed4",
        },
        {
            "piece_id": "p00000002",
            "file_path": ".",
            "offset": 4,
            "size": 2,
            "sha256": "4ca669ac3713d1f4aea07dae8dcc0d1c9867d27ea82a3ba4e6158a42206f959b",
        },
        {
            "piece_id": "p00000003",
            "file_path": ".",
            "offset": 6,
            "size": 2,
            "sha256": "fb2b7fce0940161406a6aa3e4d8b4aa6104014774ffa665743f8d9704f0eb0ec",
        },
    ]
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="canonical piece_size"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_numeric_string_fields(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["piece_size"] = "16"
    with pytest.raises(ValidationError):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_oversized_piece_size(tmp_path: Path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"payload")
    with pytest.raises(ValueError, match="piece_size"):
        create_manifest(path, piece_size=MAX_PIECE_SIZE + 1)


def test_manifest_rejects_unsorted_directories_when_rehashed(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    (root / "a").mkdir(parents=True)
    (root / "b").mkdir()
    manifest = create_manifest(root, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["directories"] = list(reversed(payload["directories"]))
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="directories must be sorted"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_unsorted_files_when_rehashed(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["files"] = list(reversed(payload["files"]))
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="files must be sorted"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_unsorted_pieces_when_rehashed(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=8)
    payload = manifest.model_dump(mode="json")
    payload["pieces"] = list(reversed(payload["pieces"]))
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="pieces must be sorted"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_noncanonical_piece_ids_when_rehashed(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["pieces"][0]["piece_id"] = "alias"
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="piece ids must be canonical"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_single_file_path_alias_when_rehashed(tmp_path: Path) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"payload")
    manifest = create_manifest(path, piece_size=4)
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["path"] = "alias.bin"
    for piece in payload["pieces"]:
        piece["file_path"] = "alias.bin"
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="file-root manifests must use"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_directory_without_declared_parent(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    payload = manifest.model_dump(mode="json")
    payload["directories"] = [{"path": "a/b"}]
    payload["files"] = []
    payload["pieces"] = []
    payload["total_size"] = 0
    _refresh_manifest_identity(payload)
    with pytest.raises(ValidationError, match="parent directory a is not declared"):
        BitswarmManifest.model_validate(payload)


def test_manifest_rejects_symlink_entries(sample_tree: Path) -> None:
    link = sample_tree / "link.txt"
    link.symlink_to(sample_tree / "a.txt")
    with pytest.raises(ValueError, match="symlinks are not supported"):
        create_manifest(sample_tree, piece_size=16)


def test_manifest_rejects_special_entries(sample_tree: Path) -> None:
    fifo = sample_tree / "pipe"
    try:
        os.mkfifo(fifo)
    except (AttributeError, PermissionError, OSError) as exc:
        pytest.skip(f"mkfifo unavailable: {exc}")
    with pytest.raises(ValueError, match="unsupported filesystem entry"):
        create_manifest(sample_tree, piece_size=16)


def test_manifest_rejects_file_symlink_root(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    target.write_text("target", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_manifest(link, piece_size=16)


def test_manifest_rejects_directory_symlink_root(sample_tree: Path, tmp_path: Path) -> None:
    link = tmp_path / "tree-link"
    link.symlink_to(sample_tree, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_manifest(link, piece_size=16)


def test_manifest_rejects_file_under_symlinked_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    target = real_parent / "target.txt"
    target.write_text("target", encoding="utf-8")
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_manifest(link_parent / "target.txt", piece_size=16)


def test_manifest_rejects_directory_under_symlinked_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    (real_parent / "tree").mkdir(parents=True)
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_manifest(link_parent / "tree", piece_size=16)


def test_manifest_rejects_relative_path_under_symlinked_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_parent = tmp_path / "real"
    tree = real_parent / "tree"
    tree.mkdir(parents=True)
    (tree / "x.txt").write_text("x", encoding="utf-8")
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent, target_is_directory=True)
    monkeypatch.chdir(link_parent)
    monkeypatch.setenv("PWD", str(link_parent))
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_manifest(Path("tree"), piece_size=16)


def test_manifest_rejects_file_swapped_to_symlink_after_enumeration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    target = root / "b.txt"
    target.write_text("local", encoding="utf-8")
    external = tmp_path / "external.txt"
    external.write_text("local", encoding="utf-8")

    import bitswarm.protocol.manifest as manifest_module

    original_iter_files = manifest_module._iter_files

    def mutating_iter_files(path: Path) -> list[Path]:
        files = original_iter_files(path)
        target.unlink()
        target.symlink_to(external)
        return files

    monkeypatch.setattr(manifest_module, "_iter_files", mutating_iter_files)
    with pytest.raises(ValueError, match="without following symlinks"):
        create_manifest(root, piece_size=16)


def test_manifest_rejects_content_mutation_during_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"aaaabbbb")

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
    with pytest.raises(ValueError, match="stable file snapshot"):
        create_manifest(target, piece_size=4)


def test_manifest_rejects_prior_file_mutation_after_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    first = root / "a.txt"
    first.write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("bravo", encoding="utf-8")

    import bitswarm.protocol.manifest as manifest_module

    original_hash = manifest_module.file_hashes_from_open_fd
    calls = 0

    def mutating_file_hash(*args, **kwargs):
        nonlocal calls
        result = original_hash(*args, **kwargs)
        calls += 1
        if calls == 1:
            first.write_text("omega", encoding="utf-8")
        return result

    monkeypatch.setattr(manifest_module, "file_hashes_from_open_fd", mutating_file_hash)
    with pytest.raises(ValueError, match="stable file tree snapshot"):
        create_manifest(root, piece_size=4)


def test_manifest_rejects_root_directory_transient_mutation_during_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("bravo", encoding="utf-8")

    import bitswarm.protocol.manifest as manifest_module

    original_hash = manifest_module.file_hashes_from_open_fd
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

    monkeypatch.setattr(manifest_module, "file_hashes_from_open_fd", mutating_file_hash)
    with pytest.raises(ValueError, match="stable file tree snapshot"):
        create_manifest(root, piece_size=4)


def test_manifest_does_not_keep_all_file_descriptors_open(tmp_path: Path) -> None:
    resource = pytest.importorskip("resource")
    root = tmp_path / "tree"
    root.mkdir()
    for index in range(80):
        (root / f"f{index:03d}.txt").write_text(f"{index}\n", encoding="utf-8")
    old_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    current_fds = len(os.listdir("/dev/fd")) if Path("/dev/fd").exists() else 16
    soft = min(old_limits[1], max(64, current_fds + 20))
    if soft <= current_fds + 5:
        pytest.skip("not enough descriptor headroom for low-fd probe")
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, old_limits[1]))
        create_manifest(root, piece_size=8)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, old_limits)


def test_manifest_allows_child_directory_with_same_name_as_root(tmp_path: Path) -> None:
    root = tmp_path / "foo"
    child = root / "foo"
    child.mkdir(parents=True)
    (child / "x.txt").write_text("x", encoding="utf-8")
    manifest = create_manifest(root, piece_size=16)
    assert [directory.path for directory in manifest.directories] == ["foo"]


def test_canonical_manifest_fixture_is_pinned(tmp_path: Path) -> None:
    root = tmp_path / "fixture"
    root.mkdir()
    (root / "alpha.txt").write_text("alpha\n", encoding="utf-8")
    (root / "beta.txt").write_text("beta\n", encoding="utf-8")
    manifest = create_manifest(root, piece_size=4, name="fixture")
    assert manifest.root_hash == "e65fe722faad8ce78624dbc20be76eb0cadbeee2052f49d19b232051d8ff2a91"
