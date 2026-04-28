from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import uvicorn

import bitswarm.cli as cli_module
from bitswarm.cli import main, safe_main
from bitswarm.client.seeder import create_seeder_app
from bitswarm.protocol.errors import TreeVerificationError
from bitswarm.protocol.manifest import create_manifest


def test_cli_manifest_and_verify(sample_tree: Path, tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    assert main(["manifest", str(sample_tree), "--out", str(manifest_path), "--piece-size", "16"]) == 0
    assert manifest_path.exists()
    assert main(["verify", str(sample_tree), str(manifest_path)]) == 0


def test_cli_seed_validates_manifest(sample_tree: Path, tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    assert main(["manifest", str(sample_tree), "--out", str(manifest_path), "--piece-size", "16"]) == 0
    (sample_tree / "a.txt").write_text("changed\n", encoding="utf-8")
    # The seeder command validates before starting uvicorn; the validation exception is the desired gate.
    with pytest.raises(TreeVerificationError):
        main(
            [
                "seed",
                str(sample_tree),
                "--manifest",
                str(manifest_path),
                "--host",
                "127.0.0.1",
                "--port",
                "1",
            ]
        )


def test_seeder_rejects_symlink_root(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    link = tmp_path / "tree-link"
    link.symlink_to(sample_tree, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_seeder_app(link, manifest=manifest)


def test_seeder_rejects_root_under_symlinked_parent(sample_tree: Path, tmp_path: Path) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    parent = sample_tree.parent
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_seeder_app(link_parent / sample_tree.name, manifest=manifest)


def test_seeder_rejects_relative_root_under_symlinked_cwd(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    parent = sample_tree.parent
    link_parent = tmp_path / "link-parent"
    link_parent.symlink_to(parent, target_is_directory=True)
    monkeypatch.chdir(link_parent)
    monkeypatch.setenv("PWD", str(link_parent))
    with pytest.raises(ValueError, match="symlink roots are not supported"):
        create_seeder_app(Path(sample_tree.name), manifest=manifest)


def test_cli_tracker_exposes_peer_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: object, *, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr(uvicorn, "run", fake_run)
    assert main(
        ["tracker", "--token", "secret", "--peer-ttl-ms", "1234", "--host", "127.0.0.1", "--port", "9"]
    ) == 0
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 9
    assert captured["app"].state.bitswarm_peer_ttl_ms == 1234


def test_cli_tracker_rejects_nonpositive_ttl() -> None:
    with pytest.raises(SystemExit):
        main(["tracker", "--token", "secret", "--peer-ttl-ms", "0"])


def test_cli_download_can_discover_tracker_peers(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)
    observed: dict[str, object] = {}

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        observed["tracker_url"] = tracker_url
        observed["manifest_id"] = manifest_id
        observed["token"] = token
        observed["expected_piece_ids"] = expected_piece_ids
        return ["http://peer-a"]

    async def fake_download_manifest(*args: Any, **kwargs: Any) -> Path:
        observed["peer_urls"] = kwargs["peer_urls"]
        return Path(kwargs["output_path"])

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    monkeypatch.setattr(cli_module, "download_manifest", fake_download_manifest)
    assert (
        main(
            [
                "download",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
                "--out",
                str(tmp_path / "downloaded"),
            ]
        )
        == 0
    )
    assert observed["manifest_id"] == manifest.manifest_id
    assert observed["expected_piece_ids"] == {piece.piece_id for piece in manifest.pieces}
    assert observed["peer_urls"] == ["http://peer-a"]


def test_tracker_peer_discovery_filters_empty_piece_maps(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            assert base_url == "http://tracker"
            assert timeout == 10.0

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            calls.append(path)
            assert headers == {"Authorization": "Bearer secret"}
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-a",
                        "peers": [
                            {
                                "peer_id": "empty-peer",
                                "base_url": "http://empty.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            },
                            {
                                "peer_id": "full-peer",
                                "base_url": "http://full.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            },
                        ],
                    }
                )
            if path == "/api/manifests/manifest-a/peers/empty-peer/pieces":
                return FakeResponse({"manifest_id": "manifest-a", "peer_id": "empty-peer", "piece_ids": []})
            if path == "/api/manifests/manifest-a/peers/full-peer/pieces":
                return FakeResponse(
                    {"manifest_id": "manifest-a", "peer_id": "full-peer", "piece_ids": ["p00000000"]}
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    sources = cli_module._tracker_peers(
        tracker_url="http://tracker",
        manifest_id="manifest-a",
        token="secret",
    )
    assert [(source.base_url, source.piece_ids) for source in sources] == [
        ("http://full.example", frozenset({"p00000000"}))
    ]
    assert calls == [
        "/api/manifests/manifest-a/peers",
        "/api/manifests/manifest-a/peers/empty-peer/pieces",
        "/api/manifests/manifest-a/peers/full-peer/pieces",
    ]


def test_tracker_peer_discovery_filters_unknown_piece_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-a",
                        "peers": [
                            {
                                "peer_id": "bad-peer",
                                "base_url": "http://bad.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            },
                            {
                                "peer_id": "good-peer",
                                "base_url": "http://good.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            },
                        ],
                    }
                )
            if path == "/api/manifests/manifest-a/peers/bad-peer/pieces":
                return FakeResponse(
                    {"manifest_id": "manifest-a", "peer_id": "bad-peer", "piece_ids": ["bogus"]}
                )
            if path == "/api/manifests/manifest-a/peers/good-peer/pieces":
                return FakeResponse(
                    {"manifest_id": "manifest-a", "peer_id": "good-peer", "piece_ids": ["p00000000"]}
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    sources = cli_module._tracker_peers(
        tracker_url="http://tracker",
        manifest_id="manifest-a",
        token="secret",
        expected_piece_ids={"p00000000"},
    )
    assert [(source.base_url, source.piece_ids) for source in sources] == [
        ("http://good.example", frozenset({"p00000000"}))
    ]


def test_tracker_peer_discovery_accepts_empty_manifest_piece_maps(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-empty/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-empty",
                        "peers": [
                            {
                                "peer_id": "empty-peer",
                                "base_url": "http://empty.example",
                                "manifests": ["manifest-empty"],
                                "updated_at_ms": 1,
                            },
                            {
                                "peer_id": "poison-peer",
                                "base_url": "http://poison.example",
                                "manifests": ["manifest-empty"],
                                "updated_at_ms": 1,
                            },
                        ],
                    }
                )
            if path == "/api/manifests/manifest-empty/peers/empty-peer/pieces":
                return FakeResponse(
                    {"manifest_id": "manifest-empty", "peer_id": "empty-peer", "piece_ids": []}
                )
            if path == "/api/manifests/manifest-empty/peers/poison-peer/pieces":
                return FakeResponse(
                    {"manifest_id": "manifest-empty", "peer_id": "poison-peer", "piece_ids": ["bogus"]}
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    sources = cli_module._tracker_peers(
        tracker_url="http://tracker",
        manifest_id="manifest-empty",
        token="secret",
        expected_piece_ids=set(),
    )
    assert [(source.base_url, source.piece_ids) for source in sources] == [
        ("http://empty.example", frozenset())
    ]


def test_tracker_peer_discovery_rejects_localhost_peer_url(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-a",
                        "peers": [
                            {
                                "peer_id": "local-peer",
                                "base_url": "http://127.0.0.1:9",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            }
                        ],
                    }
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    with pytest.raises(cli_module.ValidationError):
        cli_module._tracker_peers(
            tracker_url="http://tracker",
            manifest_id="manifest-a",
            token="secret",
            expected_piece_ids={"p00000000"},
        )


def test_tracker_peer_discovery_rejects_mismatched_peer_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-a",
                        "peers": [
                            {
                                "peer_id": "peer-a",
                                "base_url": "https://peer.example",
                                "manifests": ["other-manifest"],
                                "updated_at_ms": 1,
                            }
                        ],
                    }
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="did not advertise manifest"):
        cli_module._tracker_peers(
            tracker_url="http://tracker",
            manifest_id="manifest-a",
            token="secret",
            expected_piece_ids={"p00000000"},
        )


def test_tracker_peer_discovery_rejects_mismatched_peer_list_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-b",
                        "peers": [
                            {
                                "peer_id": "peer-a",
                                "base_url": "https://peer.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            }
                        ],
                    }
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="mismatched peer list"):
        cli_module._tracker_peers(
            tracker_url="http://tracker",
            manifest_id="manifest-a",
            token="secret",
            expected_piece_ids={"p00000000"},
        )


def test_tracker_peer_discovery_rejects_mismatched_piece_map(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            return None

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def get(self, path: str, *, headers: dict[str, str]) -> FakeResponse:
            if path == "/api/manifests/manifest-a/peers":
                return FakeResponse(
                    {
                        "manifest_id": "manifest-a",
                        "peers": [
                            {
                                "peer_id": "peer-a",
                                "base_url": "https://peer.example",
                                "manifests": ["manifest-a"],
                                "updated_at_ms": 1,
                            }
                        ],
                    }
                )
            if path == "/api/manifests/manifest-a/peers/peer-a/pieces":
                return FakeResponse(
                    {"manifest_id": "other-manifest", "peer_id": "other-peer", "piece_ids": ["p00000000"]}
                )
            raise AssertionError(path)

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    with pytest.raises(ValueError, match="mismatched piece map"):
        cli_module._tracker_peers(
            tracker_url="http://tracker",
            manifest_id="manifest-a",
            token="secret",
            expected_piece_ids={"p00000000"},
        )


def test_cli_download_reports_empty_tracker(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    sample_tree: Path,
    tmp_path: Path,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        return []

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    with pytest.raises(SystemExit):
        main(
            [
                "download",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
                "--out",
                "downloaded",
            ]
        )
    assert f"tracker returned no peers for {manifest.manifest_id}" in capsys.readouterr().err


def test_cli_safe_main_formats_validation_errors(
    sample_tree: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert safe_main(["manifest", str(sample_tree), "--out", "manifest.json", "--piece-size", "0"]) == 1
    assert "bitswarm: error:" in capsys.readouterr().err


def test_cli_safe_main_formats_http_errors(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        request = cli_module.httpx.Request("GET", "http://tracker")
        response = cli_module.httpx.Response(403, request=request)
        raise cli_module.httpx.HTTPStatusError("forbidden", request=request, response=response)

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    assert (
        safe_main(
            [
                "peers",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
            ]
        )
        == 1
    )
    assert "bitswarm: error:" in capsys.readouterr().err


def test_cli_safe_main_formats_peer_unavailable(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        return ["http://127.0.0.1:1"]

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    assert (
        safe_main(
            [
                "download",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
                "--out",
                str(tmp_path / "downloaded"),
            ]
        )
        == 1
    )
    assert "bitswarm: error:" in capsys.readouterr().err


def test_installed_console_script_uses_safe_main(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "bitswarm",
            "manifest",
            str(tmp_path),
            "--out",
            str(tmp_path / "manifest.json"),
            "--piece-size",
            "0",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parents[1],
    )
    assert result.returncode == 1
    assert "bitswarm: error:" in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_peers_lists_tracker_peers(
    sample_tree: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        assert tracker_url == "http://tracker"
        assert manifest_id == manifest.manifest_id
        assert token == "secret"
        assert expected_piece_ids == {piece.piece_id for piece in manifest.pieces}
        return ["http://peer-a", "http://peer-b"]

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    assert main(["peers", str(manifest_path), "--tracker", "http://tracker", "--token", "secret"]) == 0
    assert capsys.readouterr().out.splitlines() == ["http://peer-a", "http://peer-b"]


def test_cli_peers_rejects_raw_manifest_id(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main(["peers", "manifest-a", "--tracker", "http://tracker", "--token", "secret"])
    assert "requires a manifest file path" in capsys.readouterr().err


def test_cli_peers_filters_tracker_maps_when_manifest_path_is_supplied(
    sample_tree: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)
    observed: dict[str, object] = {}

    def fake_tracker_peers(
        *,
        tracker_url: str,
        manifest_id: str,
        token: str,
        expected_piece_ids: set[str] | None = None,
    ) -> list[str]:
        observed["manifest_id"] = manifest_id
        observed["expected_piece_ids"] = expected_piece_ids
        return ["http://peer-a"]

    monkeypatch.setattr(cli_module, "_tracker_peers", fake_tracker_peers)
    assert main(["peers", str(manifest_path), "--tracker", "http://tracker", "--token", "secret"]) == 0
    assert observed["manifest_id"] == manifest.manifest_id
    assert observed["expected_piece_ids"] == {piece.piece_id for piece in manifest.pieces}
    assert capsys.readouterr().out.splitlines() == ["http://peer-a"]


def test_cli_announce_posts_to_tracker(
    sample_tree: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)
    observed: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            observed["base_url"] = base_url
            observed["timeout"] = timeout

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def post(self, path: str, *, headers: dict[str, str], json: dict[str, object]) -> FakeResponse:
            observed["path"] = path
            observed["headers"] = headers
            observed["json"] = json
            return FakeResponse()

    monkeypatch.setattr(cli_module.httpx, "Client", FakeClient)
    assert (
        main(
            [
                "announce",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
                "--peer-secret",
                "peer-secret",
                "--peer-id",
                "peer-a",
                "--base-url",
                "http://peer-a.example",
            ]
        )
        == 0
    )
    assert observed["base_url"] == "http://tracker"
    assert observed["path"] == "/api/announces"
    assert observed["headers"] == {
        "Authorization": "Bearer secret",
        "X-Bitswarm-Peer-Secret": "peer-secret",
    }
    assert observed["json"]["manifest_id"] == manifest.manifest_id
    assert observed["json"]["base_url"] == "http://peer-a.example"


def test_cli_announce_rejects_localhost_base_url(
    sample_tree: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = create_manifest(sample_tree, piece_size=16)
    manifest_path = tmp_path / "manifest.json"
    cli_module.save_manifest(manifest, manifest_path)
    assert (
        safe_main(
            [
                "announce",
                str(manifest_path),
                "--tracker",
                "http://tracker",
                "--token",
                "secret",
                "--peer-secret",
                "peer-secret",
                "--peer-id",
                "peer-a",
                "--base-url",
                "http://127.0.0.1:8899",
            ]
        )
        == 1
    )
    assert "base_url must not target local or private addresses" in capsys.readouterr().err
