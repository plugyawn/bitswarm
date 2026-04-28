from __future__ import annotations

import socket

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from bitswarm.constants import MAX_ID_LENGTH, MAX_TRACKER_MANIFESTS_PER_PEER, MAX_TRACKER_PIECES_PER_ANNOUNCE
from bitswarm.protocol.schemas import BitswarmAnnounce, BitswarmPeer, BitswarmPieceMap
from bitswarm.tracker.app import create_tracker_app
from bitswarm.tracker.auth import PEER_SECRET_HEADER
from bitswarm.tracker.schemas import TrackerPieceMapResponse
from bitswarm.tracker.store import TrackerStore


def test_tracker_announce_and_list_peers() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    response = client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-a"},
        json=BitswarmAnnounce(
            peer_id="peer-a",
            base_url="http://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        ).model_dump(mode="json"),
    )
    assert response.status_code == 200

    peers = client.get("/api/manifests/manifest-a/peers", headers={"Authorization": "Bearer secret"})
    assert peers.status_code == 200
    assert peers.json()["peers"][0]["peer_id"] == "peer-a"

    pieces = client.get(
        "/api/manifests/manifest-a/peers/peer-a/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert pieces.status_code == 200
    assert pieces.json()["piece_ids"] == ["p00000000"]


def test_tracker_accepts_empty_piece_announce() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    response = client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-a"},
        json=BitswarmAnnounce(
            peer_id="peer-a",
            base_url="http://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[],
        ).model_dump(mode="json"),
    )
    assert response.status_code == 200
    peers = client.get("/api/manifests/manifest-a/peers", headers={"Authorization": "Bearer secret"})
    assert peers.status_code == 200
    assert peers.json()["peers"][0]["peer_id"] == "peer-a"
    pieces = client.get(
        "/api/manifests/manifest-a/peers/peer-a/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert pieces.status_code == 200
    assert pieces.json()["piece_ids"] == []


def test_tracker_requires_token_when_configured() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    response = client.get("/api/manifests/manifest-a/peers")
    assert response.status_code == 401


def test_tracker_requires_token_at_startup() -> None:
    with pytest.raises(ValueError):
        create_tracker_app(token="")


def test_tracker_requires_positive_ttl_at_startup() -> None:
    with pytest.raises(ValueError, match="peer_ttl_ms must be positive"):
        create_tracker_app(token="secret", peer_ttl_ms=0)


def test_tracker_rejects_wrong_token() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    response = client.get("/api/manifests/manifest-a/peers", headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 403


def test_tracker_rejects_peer_takeover() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    payload = BitswarmAnnounce(
        peer_id="peer-a",
        base_url="http://peer-a.example",
        manifest_id="manifest-a",
        piece_ids=["p00000000"],
    ).model_dump(mode="json")
    assert client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-a"},
        json=payload,
    ).status_code == 200
    response = client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-b"},
        json=payload,
    )
    assert response.status_code == 403


def test_tracker_expires_stale_peers() -> None:
    store = TrackerStore(peer_ttl_ms=10)
    peer = store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="http://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=100,
    )
    assert peer.peer_id == "peer-a"
    assert store.peers_for_manifest("manifest-a", at_ms=105)
    assert store.peers_for_manifest("manifest-a", at_ms=110) == []


def test_tracker_expires_availability_per_manifest() -> None:
    store = TrackerStore(peer_ttl_ms=10)
    store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="http://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=100,
    )
    peer_b = store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="http://peer-a.example",
            manifest_id="manifest-b",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=109,
    )
    assert peer_b.manifests == ["manifest-a", "manifest-b"]
    assert store.peers_for_manifest("manifest-a", at_ms=111) == []
    peers_b = store.peers_for_manifest("manifest-b", at_ms=111)
    assert [peer.peer_id for peer in peers_b] == ["peer-a"]
    assert peers_b[0].manifests == ["manifest-b"]
    assert store.pieces_for_peer(manifest_id="manifest-a", peer_id="peer-a", at_ms=111) == []
    assert store.pieces_for_peer(manifest_id="manifest-b", peer_id="peer-a", at_ms=111) == ["p00000000"]


def test_tracker_keeps_base_url_per_manifest_availability() -> None:
    store = TrackerStore(peer_ttl_ms=100)
    store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://one.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=100,
    )
    store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://two.example",
            manifest_id="manifest-b",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=101,
    )
    peers_a = store.peers_for_manifest("manifest-a", at_ms=102)
    peers_b = store.peers_for_manifest("manifest-b", at_ms=102)
    assert [str(peer.base_url) for peer in peers_a] == ["https://one.example/"]
    assert peers_a[0].manifests == ["manifest-a"]
    assert [str(peer.base_url) for peer in peers_b] == ["https://two.example/"]
    assert peers_b[0].manifests == ["manifest-b"]


def test_tracker_rejects_too_many_piece_ids() -> None:
    with pytest.raises(ValidationError, match="piece_ids"):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[f"p{index:08d}" for index in range(MAX_TRACKER_PIECES_PER_ANNOUNCE + 1)],
        )


def test_tracker_rejects_dns_alias_to_private_address(monkeypatch: pytest.MonkeyPatch) -> None:
    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "loopback.example"
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(ValidationError, match="DNS resolution"):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://loopback.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        )


def test_tracker_accepts_unresolved_public_hostname(monkeypatch: pytest.MonkeyPatch) -> None:
    import bitswarm.protocol.schemas as schemas_module

    def fake_getaddrinfo(host, *args, **kwargs):
        raise socket.gaierror("not found")

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    announce = BitswarmAnnounce(
        peer_id="peer-a",
        base_url="https://unresolved.example",
        manifest_id="manifest-a",
        piece_ids=["p00000000"],
    )
    assert str(announce.base_url) == "https://unresolved.example/"


def test_tracker_skips_peer_if_hostname_later_resolves_private(monkeypatch: pytest.MonkeyPatch) -> None:
    import bitswarm.protocol.schemas as schemas_module

    resolver_mode = "unresolved"

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "flapping.example"
        if resolver_mode == "unresolved":
            raise socket.gaierror("not found")
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(schemas_module.socket, "getaddrinfo", fake_getaddrinfo)
    store = TrackerStore(peer_ttl_ms=10_000)
    store.announce(
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://flapping.example",
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        ),
        peer_secret="peer-secret-a",
        at_ms=100,
    )
    resolver_mode = "private"
    assert store.peers_for_manifest("manifest-a", at_ms=101) == []

    client = TestClient(create_tracker_app(token="secret", store=store), raise_server_exceptions=False)
    response = client.get("/api/manifests/manifest-a/peers", headers={"Authorization": "Bearer secret"})
    assert response.status_code == 200
    assert response.json() == {"manifest_id": "manifest-a", "peers": []}


def test_tracker_rejects_overlong_control_ids() -> None:
    overlong = "x" * (MAX_ID_LENGTH + 1)
    with pytest.raises(ValidationError, match="peer_id"):
        BitswarmAnnounce(
            peer_id=overlong,
            base_url="https://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[],
        )
    with pytest.raises(ValidationError, match="manifest_id"):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifest_id=overlong,
            piece_ids=[],
        )
    with pytest.raises(ValidationError, match="piece_ids"):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[overlong],
        )
    with pytest.raises(ValidationError, match="piece_ids"):
        BitswarmPieceMap(manifest_id="manifest-a", piece_ids=[overlong])
    with pytest.raises(ValidationError, match="manifests"):
        BitswarmPeer(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifests=[overlong],
            updated_at_ms=1,
        )
    with pytest.raises(ValidationError, match="piece_ids"):
        TrackerPieceMapResponse(manifest_id="manifest-a", peer_id="peer-a", piece_ids=[overlong])


@pytest.mark.parametrize(
    "bad_id",
    [
        "peer/a",
        "peer%2Fa",
        "peer a",
        "peer?a",
        "peer#a",
        "peer:a",
        "peer@a",
    ],
)
def test_tracker_rejects_non_url_segment_control_ids(bad_id: str) -> None:
    with pytest.raises(ValidationError):
        BitswarmAnnounce(
            peer_id=bad_id,
            base_url="https://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[],
        )
    with pytest.raises(ValidationError):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifest_id=bad_id,
            piece_ids=[],
        )
    with pytest.raises(ValidationError):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url="https://peer-a.example",
            manifest_id="manifest-a",
            piece_ids=[bad_id],
        )


def test_tracker_endpoint_rejects_overlong_piece_id_item() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    overlong = "x" * (MAX_ID_LENGTH + 1)
    response = client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-a"},
        json={
            "peer_id": "peer-a",
            "base_url": "https://peer-a.example",
            "manifest_id": "manifest-a",
            "piece_ids": [overlong],
        },
    )
    assert response.status_code == 422


def test_tracker_endpoint_rejects_non_url_segment_body_ids() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    response = client.post(
        "/api/announces",
        headers={"Authorization": "Bearer secret", PEER_SECRET_HEADER: "peer-secret-a"},
        json={
            "peer_id": "peer/a",
            "base_url": "https://peer-a.example",
            "manifest_id": "manifest-a",
            "piece_ids": ["p00000000"],
        },
    )
    assert response.status_code == 422


def test_tracker_endpoint_rejects_overlong_path_ids() -> None:
    client = TestClient(create_tracker_app(token="secret"))
    overlong = "x" * (MAX_ID_LENGTH + 1)
    peers = client.get(
        f"/api/manifests/{overlong}/peers",
        headers={"Authorization": "Bearer secret"},
    )
    assert peers.status_code == 422
    pieces_manifest = client.get(
        f"/api/manifests/{overlong}/peers/peer-a/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert pieces_manifest.status_code == 422
    pieces_peer = client.get(
        f"/api/manifests/manifest-a/peers/{overlong}/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert pieces_peer.status_code == 422


def test_tracker_endpoint_rejects_non_url_segment_path_ids() -> None:
    client = TestClient(create_tracker_app(token="secret"), raise_server_exceptions=False)
    peers = client.get(
        "/api/manifests/manifest%40a/peers",
        headers={"Authorization": "Bearer secret"},
    )
    assert peers.status_code == 422
    pieces_peer = client.get(
        "/api/manifests/manifest-a/peers/peer%40a/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert pieces_peer.status_code == 422
    slash_in_path = client.get(
        "/api/manifests/manifest-a/peers/peer%2Fa/pieces",
        headers={"Authorization": "Bearer secret"},
    )
    assert slash_in_path.status_code in {404, 422}


def test_tracker_rejects_too_many_manifests_per_peer() -> None:
    store = TrackerStore(peer_ttl_ms=10_000)
    for index in range(MAX_TRACKER_MANIFESTS_PER_PEER):
        store.announce(
            BitswarmAnnounce(
                peer_id="peer-a",
                base_url="https://peer-a.example",
                manifest_id=f"manifest-{index}",
                piece_ids=[],
            ),
            peer_secret="peer-secret-a",
            at_ms=100 + index,
        )
    with pytest.raises(ValueError, match="too many advertised manifests"):
        store.announce(
            BitswarmAnnounce(
                peer_id="peer-a",
                base_url="https://peer-a.example",
                manifest_id="manifest-overflow",
                piece_ids=[],
            ),
            peer_secret="peer-secret-a",
            at_ms=1000,
        )


def test_tracker_rejects_peer_takeover_after_expiry() -> None:
    store = TrackerStore(peer_ttl_ms=10)
    payload = BitswarmAnnounce(
        peer_id="peer-a",
        base_url="http://peer-a.example",
        manifest_id="manifest-a",
        piece_ids=["p00000000"],
    )
    store.announce(payload, peer_secret="peer-secret-a", at_ms=100)
    assert store.peers_for_manifest("manifest-a", at_ms=111) == []
    with pytest.raises(PermissionError):
        store.announce(payload, peer_secret="peer-secret-b", at_ms=112)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://127.0.0.1:8899",
        "http://127.0.0.2:8899",
        "http://0.0.0.0:8899",
        "http://localhost:8899",
        "http://localhost.:8899",
        "http://[::1]:8899",
        "http://[::ffff:127.0.0.1]:8899",
        "http://10.0.0.1:8899",
        "http://192.168.1.10:8899",
        "http://169.254.1.1:8899",
        "http://peer-a:8899",
        "https://peer-a.example/base",
        "https://peer-a.example/?x=1",
        "https://peer-a.example/#fragment",
        "https://user:pass@peer-a.example",
        "https://user@peer-a.example",
    ],
)
def test_tracker_peer_urls_reject_local_or_private_targets(base_url: str) -> None:
    with pytest.raises(ValidationError, match="base_url"):
        BitswarmAnnounce(
            peer_id="peer-a",
            base_url=base_url,
            manifest_id="manifest-a",
            piece_ids=["p00000000"],
        )
    with pytest.raises(ValidationError, match="base_url"):
        BitswarmPeer(
            peer_id="peer-a",
            base_url=base_url,
            manifests=["manifest-a"],
            updated_at_ms=1,
        )
