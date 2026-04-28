from __future__ import annotations

from pathlib import Path

import httpcore
import httpx
import pytest

from bitswarm.client.cache import promote_verified_tree
from bitswarm.client.downloader import download_manifest, tracker_peer_source
from bitswarm.client.seeder import create_seeder_app
from bitswarm.client.transport import PinnedDNSAsyncNetworkBackend
from bitswarm.protocol.errors import (
    CachePromotionError,
    PieceUnavailableError,
    PieceVerificationError,
    TreeVerificationError,
)
from bitswarm.protocol.manifest import create_manifest
from bitswarm.protocol.verifier import verify_manifest_tree


async def test_client_downloads_and_promotes_verified_tree(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=tmp_path / "downloaded",
            client=client,
        )
    verify_manifest_tree(destination, manifest)
    assert not (tmp_path / f".downloaded.{manifest.manifest_id}.partial").exists()


async def test_client_reconstructs_empty_files(sample_tree: Path, tmp_path: Path) -> None:
    (sample_tree / "empty.txt").touch()
    manifest = create_manifest(sample_tree, piece_size=9)
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=tmp_path / "downloaded",
            client=client,
        )
    assert (destination / "empty.txt").exists()
    verify_manifest_tree(destination, manifest)


async def test_client_rejects_corrupt_piece(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"corrupt")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://peer") as client:
        with pytest.raises(PieceVerificationError):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=tmp_path / "downloaded",
                client=client,
            )
    assert not (tmp_path / "downloaded").exists()
    assert not list(tmp_path.glob(".downloaded.*.partial"))


async def test_client_rejects_oversized_piece_before_buffering(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    first_piece = manifest.pieces[0]

    class OversizedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * first_piece.size
            yield b"y"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=OversizedStream())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://peer") as client:
        with pytest.raises(PieceVerificationError, match="exceeded declared size"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=tmp_path / "downloaded",
                client=client,
            )
    assert not (tmp_path / "downloaded").exists()


async def test_client_falls_back_after_corrupt_peer(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    good = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))

    async def corrupt_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"corrupt")

    corrupt = httpx.MockTransport(corrupt_handler)

    async def router(request: httpx.Request) -> httpx.Response:
        if request.url.host == "bad":
            return await corrupt.handle_async_request(request)
        return await good.handle_async_request(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(router)) as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://bad", "http://good"],
            output_path=tmp_path / "downloaded",
            client=client,
        )
    verify_manifest_tree(destination, manifest)


async def test_client_skips_peers_that_do_not_advertise_piece(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=5)
    good = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    first_piece = manifest.pieces[0]
    advertised_by_good = {piece.piece_id for piece in manifest.pieces}
    bad_calls: list[str] = []

    async def router(request: httpx.Request) -> httpx.Response:
        if request.url.host == "bad.example":
            bad_calls.append(request.url.path)
            return httpx.Response(503, content=b"not serving this piece")
        return await good.handle_async_request(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(router)) as client:
        destination = await download_manifest(
            manifest,
            peer_urls=[
                tracker_peer_source("https://bad.example", {first_piece.piece_id}),
                tracker_peer_source("https://good.example", advertised_by_good),
            ],
            output_path=tmp_path / "downloaded",
            client=client,
            allow_unpinned_tracker_client=True,
        )

    verify_manifest_tree(destination, manifest)
    assert bad_calls == [f"/api/manifests/{manifest.manifest_id}/pieces/{first_piece.piece_id}"]


@pytest.mark.parametrize(
    ("peer_url", "message"),
    [
        ("http://user:pass@peer.example", "username or password"),
        ("http://peer.example/base", "path, query, or fragment"),
        ("http://peer.example?x=1", "path, query, or fragment"),
    ],
)
async def test_client_direct_peer_urls_reject_non_origin_inputs(
    sample_tree: Path,
    tmp_path: Path,
    peer_url: str,
    message: str,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ValueError, match=message):
            await download_manifest(
                manifest,
                peer_urls=[peer_url],
                output_path=tmp_path / "downloaded",
                client=client,
            )


async def test_client_direct_peer_urls_allow_explicit_local_origins(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport) as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://127.0.0.1:8899"],
            output_path=tmp_path / "downloaded",
            client=client,
        )
    verify_manifest_tree(destination, manifest)


def test_tracker_peer_source_captures_validated_dns_pin(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "peer.example"
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    peer_source = tracker_peer_source("https://peer.example", {"p00000000"})
    assert peer_source.pin_host == "peer.example"
    assert peer_source.pinned_ips == frozenset({"93.184.216.34"})


async def test_pinned_network_backend_connects_to_validated_ip() -> None:
    class FakeBackend:
        def __init__(self) -> None:
            self.connected_host: str | None = None

        async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
            self.connected_host = host
            return object()

        async def connect_unix_socket(self, path, timeout=None, socket_options=None):
            raise AssertionError("unexpected unix socket connection")

        async def sleep(self, seconds):
            return None

    fake_backend = FakeBackend()
    backend = PinnedDNSAsyncNetworkBackend(
        {"peer.example": ("93.184.216.34",)},
        delegate=fake_backend,  # type: ignore[arg-type]
    )
    await backend.connect_tcp("peer.example", 443)
    assert fake_backend.connected_host == "93.184.216.34"


async def test_pinned_network_backend_tries_all_validated_ips() -> None:
    class FakeBackend:
        def __init__(self) -> None:
            self.connected_hosts: list[str] = []

        async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
            self.connected_hosts.append(host)
            if host == "93.184.216.34":
                raise httpcore.ConnectError("first pin failed")
            return object()

        async def connect_unix_socket(self, path, timeout=None, socket_options=None):
            raise AssertionError("unexpected unix socket connection")

        async def sleep(self, seconds):
            return None

    fake_backend = FakeBackend()
    backend = PinnedDNSAsyncNetworkBackend(
        {"peer.example": ("93.184.216.34", "1.1.1.1")},
        delegate=fake_backend,  # type: ignore[arg-type]
    )
    await backend.connect_tcp("peer.example", 443)
    assert fake_backend.connected_hosts == ["93.184.216.34", "1.1.1.1"]


async def test_pinned_network_backend_fails_closed_without_validated_ip() -> None:
    backend = PinnedDNSAsyncNetworkBackend({"unresolved.example": ()})
    with pytest.raises(httpcore.ConnectError, match="no validated DNS address"):
        await backend.connect_tcp("unresolved.example", 80)


async def test_default_download_fails_closed_for_unresolved_tracker_peer(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "unresolved.example"
        raise socket.gaierror("not found")

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    manifest = create_manifest(sample_tree, piece_size=9)
    peer_source = tracker_peer_source("https://unresolved.example", {manifest.pieces[0].piece_id})
    with pytest.raises(PieceUnavailableError, match="no validated DNS address"):
        await download_manifest(
            manifest,
            peer_urls=[peer_source],
            output_path=tmp_path / "downloaded",
        )


async def test_custom_client_rejected_for_tracker_sources_without_explicit_opt_out(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "peer.example"
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    manifest = create_manifest(sample_tree, piece_size=9)
    peer_source = tracker_peer_source("https://peer.example", {manifest.pieces[0].piece_id})
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ValueError, match="custom clients bypass tracker DNS pinning"):
            await download_manifest(
                manifest,
                peer_urls=[peer_source],
                output_path=tmp_path / "downloaded",
                client=client,
            )


async def test_custom_client_opt_out_is_explicit_for_tracker_sources(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "peer.example"
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    manifest = create_manifest(sample_tree, piece_size=9)
    peer_source = tracker_peer_source("https://peer.example", {piece.piece_id for piece in manifest.pieces})
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport) as client:
        destination = await download_manifest(
            manifest,
            peer_urls=[peer_source],
            output_path=tmp_path / "downloaded",
            client=client,
            allow_unpinned_tracker_client=True,
        )
    verify_manifest_tree(destination, manifest)


async def test_seeder_rejects_post_start_symlink_mutation(tmp_path: Path) -> None:
    source = tmp_path / "served"
    source.mkdir()
    (source / "a.txt").write_text("alpha", encoding="utf-8")
    secret = tmp_path / "secret.txt"
    secret.write_text("SECRET", encoding="utf-8")
    manifest = create_manifest(source, piece_size=32)
    app = create_seeder_app(source, manifest=manifest)
    (source / "a.txt").unlink()
    (source / "a.txt").symlink_to(secret)
    transport = httpx.ASGITransport(app=app)
    piece_id = manifest.pieces[0].piece_id
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        response = await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}")
    assert response.status_code == 409
    assert response.content != b"SECRET"


async def test_seeder_routes_reject_overlong_or_non_segment_ids(sample_tree: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    app = create_seeder_app(sample_tree, manifest=manifest)
    overlong = "x" * 129
    piece_id = manifest.pieces[0].piece_id
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        responses = [
            await client.get(f"/api/manifests/{overlong}"),
            await client.get(f"/api/manifests/{overlong}/piece-map"),
            await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{overlong}"),
            await client.get("/api/manifests/bad%40id"),
            await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/bad%40id"),
            await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}"),
        ]
    assert [response.status_code for response in responses] == [422, 422, 422, 422, 422, 200]


async def test_seeder_rejects_post_verify_content_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "served"
    source.mkdir()
    served_file = source / "a.txt"
    served_file.write_text("alpha", encoding="utf-8")
    manifest = create_manifest(source, piece_size=32)
    app = create_seeder_app(source, manifest=manifest)
    import bitswarm.client.seeder as seeder_module

    original_verify = seeder_module.verify_manifest_tree
    mutated = False

    def mutating_verify(root: Path, checked_manifest) -> None:
        nonlocal mutated
        original_verify(root, checked_manifest)
        if not mutated:
            served_file.write_text("omega", encoding="utf-8")
            mutated = True

    monkeypatch.setattr(seeder_module, "verify_manifest_tree", mutating_verify)
    transport = httpx.ASGITransport(app=app)
    piece_id = manifest.pieces[0].piece_id
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        response = await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}")
    assert response.status_code == 409
    assert response.content != b"omega"


async def test_seeder_rejects_post_verify_parent_symlink_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "served"
    (source / "sub").mkdir(parents=True)
    served_file = source / "sub" / "a.txt"
    served_file.write_text("alpha", encoding="utf-8")
    escape = source / "escape"
    escape.mkdir()
    (escape / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(source, piece_size=32)
    app = create_seeder_app(source, manifest=manifest)
    import bitswarm.client.seeder as seeder_module

    original_verify = seeder_module.verify_manifest_tree
    mutated = False

    def mutating_verify(root: Path, checked_manifest) -> None:
        nonlocal mutated
        original_verify(root, checked_manifest)
        if not mutated:
            served_file.unlink()
            (source / "sub").rmdir()
            (source / "sub").symlink_to(escape, target_is_directory=True)
            mutated = True

    monkeypatch.setattr(seeder_module, "verify_manifest_tree", mutating_verify)
    transport = httpx.ASGITransport(app=app)
    piece_id = next(piece.piece_id for piece in manifest.pieces if piece.file_path == "sub/a.txt")
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        response = await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}")
    assert response.status_code == 409


async def test_seeder_rejects_post_verify_root_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "served"
    source.mkdir()
    (source / "a.txt").write_text("alpha", encoding="utf-8")
    (source / "b.txt").write_text("bravo", encoding="utf-8")
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    (replacement / "a.txt").write_text("alpha", encoding="utf-8")
    (replacement / "b.txt").write_text("not-the-manifest-tree", encoding="utf-8")
    old_source = tmp_path / "served-old"
    manifest = create_manifest(source, piece_size=32)
    app = create_seeder_app(source, manifest=manifest)
    import bitswarm.client.seeder as seeder_module

    original_verify = seeder_module.verify_manifest_tree
    swapped = False

    def swapping_verify(root: Path, checked_manifest) -> None:
        nonlocal swapped
        original_verify(root, checked_manifest)
        if not swapped:
            source.rename(old_source)
            replacement.rename(source)
            swapped = True

    monkeypatch.setattr(seeder_module, "verify_manifest_tree", swapping_verify)
    transport = httpx.ASGITransport(app=app)
    piece_id = next(piece.piece_id for piece in manifest.pieces if piece.file_path == "a.txt")
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        response = await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}")
    assert response.status_code == 409


async def test_seeder_rejects_post_read_root_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "served"
    source.mkdir()
    (source / "a.txt").write_text("alpha", encoding="utf-8")
    (source / "b.txt").write_text("bravo", encoding="utf-8")
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    (replacement / "a.txt").write_text("alpha", encoding="utf-8")
    (replacement / "b.txt").write_text("not-the-manifest-tree", encoding="utf-8")
    old_source = tmp_path / "served-old"
    manifest = create_manifest(source, piece_size=32)
    app = create_seeder_app(source, manifest=manifest)
    import bitswarm.client.seeder as seeder_module

    original_read_piece = seeder_module.read_piece
    swapped = False

    def swapping_read_piece(root: Path, piece):
        nonlocal swapped
        data = original_read_piece(root, piece)
        if not swapped:
            source.rename(old_source)
            replacement.rename(source)
            swapped = True
        return data

    monkeypatch.setattr(seeder_module, "read_piece", swapping_read_piece)
    transport = httpx.ASGITransport(app=app)
    piece_id = next(piece.piece_id for piece in manifest.pieces if piece.file_path == "a.txt")
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        response = await client.get(f"/api/manifests/{manifest.manifest_id}/pieces/{piece_id}")
    assert response.status_code == 409


async def test_failed_download_preserves_existing_destination(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    destination = tmp_path / "downloaded"
    destination.mkdir()
    (destination / "old.txt").write_text("keep me", encoding="utf-8")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"corrupt")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://peer") as client:
        with pytest.raises(PieceVerificationError):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=destination,
                client=client,
            )
    assert (destination / "old.txt").read_text(encoding="utf-8") == "keep me"


async def test_download_rejects_file_manifest_to_existing_directory(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    destination = tmp_path / "downloaded"
    destination.mkdir()
    (destination / "old.txt").write_text("keep me", encoding="utf-8")
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError, match="cannot replace non-file"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=destination,
                client=client,
            )
    assert destination.is_dir()
    assert (destination / "old.txt").read_text(encoding="utf-8") == "keep me"


async def test_download_rejects_directory_manifest_to_existing_file(
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=9)
    destination = tmp_path / "downloaded"
    destination.write_text("keep me", encoding="utf-8")
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError, match="cannot replace non-directory"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=destination,
                client=client,
            )
    assert destination.is_file()
    assert destination.read_text(encoding="utf-8") == "keep me"


async def test_download_rejects_output_symlink(sample_tree: Path, tmp_path: Path) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    real_target = tmp_path / "real-target"
    real_target.write_text("keep me", encoding="utf-8")
    output_link = tmp_path / "alias"
    output_link.symlink_to(real_target)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(ValueError, match="output path must not be a symlink"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=output_link,
                client=client,
            )
    assert real_target.read_text(encoding="utf-8") == "keep me"
    assert output_link.is_symlink()


async def test_download_rejects_output_symlink_ancestor(sample_tree: Path, tmp_path: Path) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(real_parent, target_is_directory=True)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(ValueError, match="symlink ancestors"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=link_parent / "downloaded.txt",
                client=client,
            )
    assert not (real_parent / "downloaded.txt").exists()


async def test_download_rejects_dangling_output_symlink_ancestor(sample_tree: Path, tmp_path: Path) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    escape = tmp_path / "escape"
    link_parent = tmp_path / "dangling-parent"
    link_parent.symlink_to(escape / "nested", target_is_directory=True)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(ValueError, match="symlink ancestors"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=link_parent / "downloaded.txt",
                client=client,
            )
    assert not escape.exists()


async def test_download_allows_top_level_tmp_compat_symlink(sample_tree: Path, tmp_path: Path) -> None:
    tmp_root = Path("/tmp")
    if not tmp_root.exists() or not tmp_root.is_symlink():
        pytest.skip("/tmp is not a top-level compatibility symlink on this platform")
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    output_file = tmp_root / f"bitswarm-{tmp_path.name}-downloaded.txt"
    if output_file.exists() or output_file.is_symlink():
        output_file.unlink()
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
            destination = await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=output_file,
                client=client,
            )
        verify_manifest_tree(destination, manifest)
    finally:
        if output_file.exists() or output_file.is_symlink():
            output_file.unlink()


async def test_download_rejects_missing_parent_symlink_race(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    safe_root = tmp_path / "safe"
    safe_root.mkdir()
    escape = tmp_path / "escape"
    escape.mkdir()
    missing_parent = safe_root / "newdir"

    import bitswarm.client.cache as cache_module

    original_safe_create_parent = cache_module._safe_create_parent

    def mutating_safe_create_parent(parent: Path) -> None:
        if parent == missing_parent:
            missing_parent.symlink_to(escape, target_is_directory=True)
        original_safe_create_parent(parent)

    monkeypatch.setattr(cache_module, "_safe_create_parent", mutating_safe_create_parent)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=missing_parent / "downloaded.txt",
                client=client,
            )
    assert not list(escape.rglob("*"))


async def test_download_rejects_staging_symlink_swap(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=4)
    transport = httpx.ASGITransport(app=create_seeder_app(sample_tree, manifest=manifest))
    escape_target = tmp_path / "escape-target"
    escape_target.mkdir()

    async def swap_staging(done: int, total: int, piece_id: str) -> None:
        if done != 1:
            return
        matches = list(tmp_path.glob(f".downloaded.{manifest.manifest_id}.*.partial"))
        assert len(matches) == 1
        staging = matches[0]
        if staging.is_dir():
            for child in staging.rglob("*"):
                if child.is_file():
                    child.unlink()
            for child in sorted(staging.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            staging.rmdir()
        else:
            staging.unlink()
        staging.symlink_to(escape_target, target_is_directory=True)

    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError, match="staging path changed"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=tmp_path / "downloaded",
                client=client,
                progress_cb=swap_staging,
            )
    assert not any(escape_target.rglob("*"))
    assert not list(tmp_path.glob(f".downloaded.{manifest.manifest_id}.*.partial"))


async def test_download_rejects_nested_staging_symlink_swap(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    (source_dir / "sub").mkdir(parents=True)
    (source_dir / "a.txt").write_text("alpha", encoding="utf-8")
    (source_dir / "sub" / "b.txt").write_text("beta", encoding="utf-8")
    manifest = create_manifest(source_dir, piece_size=32)
    transport = httpx.ASGITransport(app=create_seeder_app(source_dir, manifest=manifest))
    escape_target = tmp_path / "escape-target"
    escape_target.mkdir()

    async def swap_nested_dir(done: int, total: int, piece_id: str) -> None:
        if done != 1:
            return
        matches = list(tmp_path.glob(f".downloaded.{manifest.manifest_id}.*.partial"))
        assert len(matches) == 1
        nested = matches[0] / "sub"
        nested.rmdir()
        nested.symlink_to(escape_target, target_is_directory=True)

    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError, match="without following symlinks"):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=tmp_path / "downloaded",
                client=client,
                progress_cb=swap_nested_dir,
            )
    assert not any(escape_target.rglob("*"))


async def test_single_file_download(sample_tree: Path, tmp_path: Path) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    output_file = tmp_path / "out.txt"
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=output_file,
            client=client,
        )
    assert destination == output_file.resolve()
    assert output_file.read_text(encoding="utf-8") == source_file.read_text(encoding="utf-8")
    verify_manifest_tree(output_file, manifest)


async def test_single_file_download_to_suffixless_path(sample_tree: Path, tmp_path: Path) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    output_file = tmp_path / "downloaded"
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=output_file,
            client=client,
        )
    assert destination == output_file.resolve()
    assert output_file.is_file()
    assert output_file.read_text(encoding="utf-8") == source_file.read_text(encoding="utf-8")
    verify_manifest_tree(output_file, manifest)


async def test_one_file_directory_download_to_suffix_path_stays_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "only").write_text("directory-root", encoding="utf-8")
    manifest = create_manifest(source_dir, piece_size=5)
    transport = httpx.ASGITransport(app=create_seeder_app(source_dir, manifest=manifest))
    output_dir = tmp_path / "downloaded.txt"
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=output_dir,
            client=client,
        )
    assert destination == output_dir.resolve()
    assert output_dir.is_dir()
    assert (output_dir / "only").read_text(encoding="utf-8") == "directory-root"
    verify_manifest_tree(output_dir, manifest)


async def test_empty_directory_download_preserved(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    (source_dir / "empty").mkdir(parents=True)
    manifest = create_manifest(source_dir, piece_size=5)
    transport = httpx.ASGITransport(app=create_seeder_app(source_dir, manifest=manifest))
    output_dir = tmp_path / "downloaded"
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=output_dir,
            client=client,
        )
    assert (destination / "empty").is_dir()
    verify_manifest_tree(destination, manifest)


async def test_completely_empty_directory_download_preserved(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    manifest = create_manifest(source_dir, piece_size=5)
    transport = httpx.ASGITransport(app=create_seeder_app(source_dir, manifest=manifest))
    output_dir = tmp_path / "downloaded"
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        destination = await download_manifest(
            manifest,
            peer_urls=["http://peer"],
            output_path=output_dir,
            client=client,
        )
    assert destination.is_dir()
    assert list(destination.iterdir()) == []
    verify_manifest_tree(destination, manifest)


def test_promotion_rejects_post_verify_staging_content_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    target_file = staging / "a.txt"
    target_file.write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"

    import bitswarm.client.cache as cache_module

    original_verify = cache_module.verify_manifest_tree
    calls = 0

    def mutating_verify(root: Path, checked_manifest) -> None:
        nonlocal calls
        original_verify(root, checked_manifest)
        calls += 1
        if calls == 1:
            target_file.write_text("omega", encoding="utf-8")

    monkeypatch.setattr(cache_module, "verify_manifest_tree", mutating_verify)
    with pytest.raises(CachePromotionError):
        promote_verified_tree(staging, destination, manifest)
    assert not destination.exists()


def test_promotion_rejects_hard_link_aliasing(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    target_file = staging / "a.txt"
    target_file.write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    external = tmp_path / "external.txt"
    external.write_text("alpha", encoding="utf-8")
    target_file.unlink()
    target_file.hardlink_to(external)
    destination = tmp_path / "destination"
    with pytest.raises(TreeVerificationError, match="hard-linked files are not supported"):
        promote_verified_tree(staging, destination, manifest)
    assert not destination.exists()


def test_promotion_does_not_publish_post_verify_staging_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"

    import bitswarm.client.cache as cache_module

    original_verify = cache_module.verify_manifest_tree
    calls = 0

    def mutating_verify(root: Path, checked_manifest) -> None:
        nonlocal calls
        original_verify(root, checked_manifest)
        calls += 1
        if calls == 1:
            (staging / "injected.txt").write_text("bad", encoding="utf-8")

    monkeypatch.setattr(cache_module, "verify_manifest_tree", mutating_verify)
    promote_verified_tree(staging, destination, manifest)
    verify_manifest_tree(destination, manifest)
    assert not (destination / "injected.txt").exists()


def test_promotion_rejects_promote_root_mutation_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"

    import bitswarm.client.cache as cache_module

    original_replace = cache_module._replace_verified_path_no_symlinks
    mutated = False

    def mutating_replace(source: Path, target: Path, checked_manifest, fingerprint) -> None:
        nonlocal mutated
        if source.name.endswith(".promote") and not mutated:
            (source / "a.txt").write_text("omega", encoding="utf-8")
            mutated = True
        original_replace(source, target, checked_manifest, fingerprint)

    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", mutating_replace)
    with pytest.raises(CachePromotionError):
        promote_verified_tree(staging, destination, manifest)
    assert not destination.exists()


def test_promotion_restores_backup_if_promotion_root_becomes_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "a.txt").write_text("alpha", encoding="utf-8")

    import bitswarm.client.cache as cache_module

    original_replace = cache_module._replace_verified_path_no_symlinks
    swapped = False

    def swapping_replace(source: Path, target: Path, checked_manifest, fingerprint) -> None:
        nonlocal swapped
        if source.name.endswith(".promote") and not swapped:
            cache_module.clear_staging(source)
            source.symlink_to(tmp_path, target_is_directory=True)
            swapped = True
        original_replace(source, target, checked_manifest, fingerprint)

    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", swapping_replace)
    with pytest.raises(CachePromotionError):
        promote_verified_tree(staging, destination, manifest)
    verify_manifest_tree(destination, manifest)
    assert (destination / "a.txt").read_text(encoding="utf-8") == "alpha"
    assert not list(tmp_path.glob(".destination.*.old"))


def test_promotion_restores_backup_if_destination_becomes_symlink_during_unwind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "a.txt").write_text("alpha", encoding="utf-8")
    escape = tmp_path / "escape"
    escape.mkdir()

    import bitswarm.client.cache as cache_module

    original_replace = cache_module._replace_verified_path_no_symlinks
    injected = False

    def failing_replace(source: Path, target: Path, checked_manifest, fingerprint) -> None:
        nonlocal injected
        if not injected:
            target.symlink_to(escape, target_is_directory=True)
            injected = True
        raise OSError("forced install failure")

    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", failing_replace)
    with pytest.raises(CachePromotionError):
        promote_verified_tree(staging, destination, manifest)
    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", original_replace)
    verify_manifest_tree(destination, manifest)
    assert not destination.is_symlink()
    assert (destination / "a.txt").read_text(encoding="utf-8") == "alpha"
    assert not list(tmp_path.glob(".destination.*.old"))


def test_promotion_restores_backup_after_destination_symlink_reinsert_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.txt").write_text("alpha", encoding="utf-8")
    manifest = create_manifest(staging, piece_size=32)
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "a.txt").write_text("alpha", encoding="utf-8")
    escape = tmp_path / "escape"
    escape.mkdir()

    import bitswarm.client.cache as cache_module

    original_replace = cache_module._replace_verified_path_no_symlinks
    original_clear = cache_module.clear_staging
    injected_install_symlink = False
    reinjected_restore_symlink = False

    def failing_replace(source: Path, target: Path, checked_manifest, fingerprint) -> None:
        nonlocal injected_install_symlink
        if not injected_install_symlink:
            target.symlink_to(escape, target_is_directory=True)
            injected_install_symlink = True
        raise OSError("forced install failure")

    def racing_clear(path: Path) -> None:
        nonlocal reinjected_restore_symlink
        original_clear(path)
        if path == destination and not reinjected_restore_symlink:
            destination.symlink_to(escape, target_is_directory=True)
            reinjected_restore_symlink = True

    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", failing_replace)
    monkeypatch.setattr(cache_module, "clear_staging", racing_clear)
    with pytest.raises(CachePromotionError):
        promote_verified_tree(staging, destination, manifest)
    monkeypatch.setattr(cache_module, "_replace_verified_path_no_symlinks", original_replace)
    monkeypatch.setattr(cache_module, "clear_staging", original_clear)
    verify_manifest_tree(destination, manifest)
    assert not destination.is_symlink()
    assert (destination / "a.txt").read_text(encoding="utf-8") == "alpha"
    assert not list(tmp_path.glob(".destination.*.old"))


async def test_download_rejects_parent_swap_after_staging_parent_check(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_file = sample_tree / "a.txt"
    manifest = create_manifest(source_file, piece_size=5)
    safe_parent = tmp_path / "safe"
    safe_parent.mkdir()
    escape = tmp_path / "escape"
    escape.mkdir()

    import bitswarm.client.cache as cache_module

    original_safe_create_parent = cache_module._safe_create_parent
    swapped = False

    def swapping_safe_create_parent(parent: Path) -> None:
        nonlocal swapped
        original_safe_create_parent(parent)
        if parent == safe_parent and not swapped:
            safe_parent.rmdir()
            safe_parent.symlink_to(escape, target_is_directory=True)
            swapped = True

    monkeypatch.setattr(cache_module, "_safe_create_parent", swapping_safe_create_parent)
    transport = httpx.ASGITransport(app=create_seeder_app(source_file, manifest=manifest))
    async with httpx.AsyncClient(transport=transport, base_url="http://peer") as client:
        with pytest.raises(CachePromotionError):
            await download_manifest(
                manifest,
                peer_urls=["http://peer"],
                output_path=safe_parent / "downloaded.txt",
                client=client,
            )
    assert not any(escape.rglob("*"))


def test_replace_rejects_parent_swap_after_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    safe = tmp_path / "safe"
    safe.mkdir()
    source = safe / "source.txt"
    source.write_text("alpha", encoding="utf-8")
    destination = safe / "destination.txt"
    escape = tmp_path / "escape"
    escape.mkdir()
    (escape / "source.txt").write_text("omega", encoding="utf-8")

    import bitswarm.client.cache as cache_module

    original_resolve = cache_module.resolve_target_without_symlink_ancestors
    calls = 0
    swapped = False

    def swapping_resolve(path: Path) -> Path:
        nonlocal calls, swapped
        resolved = original_resolve(path)
        calls += 1
        if calls == 2 and not swapped:
            safe.rename(tmp_path / "safe-old")
            safe.symlink_to(escape, target_is_directory=True)
            swapped = True
        return resolved

    monkeypatch.setattr(cache_module, "resolve_target_without_symlink_ancestors", swapping_resolve)
    with pytest.raises(OSError):
        cache_module._replace_path_no_symlinks(source, destination)
    assert (tmp_path / "safe-old" / "source.txt").read_text(encoding="utf-8") == "alpha"
    assert not (escape / "destination.txt").exists()
