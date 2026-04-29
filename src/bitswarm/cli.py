"""Bitswarm command line interface."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import httpx
import uvicorn
from pydantic import ValidationError

from bitswarm.ariang.app import create_ariang_app, is_safe_local_bind
from bitswarm.client.downloader import PeerSource, download_manifest, tracker_peer_source
from bitswarm.client.seeder import create_seeder_app
from bitswarm.protocol.errors import BitswarmError
from bitswarm.protocol.manifest import create_manifest, load_manifest, save_manifest
from bitswarm.protocol.peer import full_piece_map
from bitswarm.protocol.schemas import BitswarmAnnounce
from bitswarm.protocol.verifier import verify_manifest_tree
from bitswarm.tracker.app import create_tracker_app
from bitswarm.tracker.schemas import TrackerPeersResponse, TrackerPieceMapResponse


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _manifest_identity_from_arg(value: str) -> tuple[str, set[str] | None]:
    path = Path(value)
    if not path.exists():
        return value, None
    manifest = load_manifest(path)
    return manifest.manifest_id, {piece.piece_id for piece in manifest.pieces}


def _tracker_peers(
    *,
    tracker_url: str,
    manifest_id: str,
    token: str,
    expected_piece_ids: set[str] | None = None,
) -> list[PeerSource]:
    with httpx.Client(base_url=tracker_url, timeout=10.0) as client:
        response = client.get(f"/api/manifests/{manifest_id}/peers", headers=_auth_headers(token))
        response.raise_for_status()
        peer_response = TrackerPeersResponse.model_validate(response.json())
        if peer_response.manifest_id != manifest_id:
            raise ValueError("tracker returned mismatched peer list")
        peers = peer_response.peers
        available_sources: list[PeerSource] = []
        for peer in peers:
            if manifest_id not in peer.manifests:
                raise ValueError(f"tracker peer {peer.peer_id} did not advertise manifest {manifest_id}")
            piece_response = client.get(
                f"/api/manifests/{manifest_id}/peers/{peer.peer_id}/pieces",
                headers=_auth_headers(token),
            )
            piece_response.raise_for_status()
            piece_map = TrackerPieceMapResponse.model_validate(piece_response.json())
            if piece_map.manifest_id != manifest_id or piece_map.peer_id != peer.peer_id:
                raise ValueError(f"tracker returned mismatched piece map for peer {peer.peer_id}")
            piece_ids = set(piece_map.piece_ids)
            if expected_piece_ids is not None:
                if not expected_piece_ids:
                    if not piece_ids:
                        available_sources.append(tracker_peer_source(str(peer.base_url), piece_ids))
                    continue
                piece_ids &= expected_piece_ids
            if piece_ids:
                available_sources.append(tracker_peer_source(str(peer.base_url), piece_ids))
        return available_sources


def main(argv: list[str] | None = None) -> int:
    return _main(argv)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bitswarm")
    sub = parser.add_subparsers(dest="command", required=True)

    manifest_parser = sub.add_parser("manifest", help="Create a manifest for a file tree.")
    manifest_parser.add_argument("path")
    manifest_parser.add_argument("--out", required=True)
    manifest_parser.add_argument("--piece-size", type=int, default=1_048_576)
    manifest_parser.add_argument("--name", default=None)

    verify_parser = sub.add_parser("verify", help="Verify a file tree against a manifest.")
    verify_parser.add_argument("path")
    verify_parser.add_argument("manifest")

    seed_parser = sub.add_parser("seed", help="Serve a file tree as a local seeder.")
    seed_parser.add_argument("path")
    seed_parser.add_argument("--manifest", default=None)
    seed_parser.add_argument("--host", default="127.0.0.1")
    seed_parser.add_argument("--port", type=int, default=8899)

    download_parser = sub.add_parser("download", help="Download and verify from one or more peers.")
    download_parser.add_argument("manifest")
    download_parser.add_argument("--peer", action="append", default=[])
    download_parser.add_argument("--tracker", default=None)
    download_parser.add_argument("--token", default=None)
    download_parser.add_argument("--out", required=True)

    tracker_parser = sub.add_parser("tracker", help="Run a lightweight tracker.")
    tracker_parser.add_argument("--host", default="127.0.0.1")
    tracker_parser.add_argument("--port", type=int, default=8898)
    tracker_parser.add_argument("--token", required=True)
    tracker_parser.add_argument("--peer-ttl-ms", type=_positive_int, default=300_000)

    announce_parser = sub.add_parser("announce", help="Announce a local seeder to a tracker.")
    announce_parser.add_argument("manifest")
    announce_parser.add_argument("--tracker", required=True)
    announce_parser.add_argument("--token", required=True)
    announce_parser.add_argument("--peer-secret", required=True)
    announce_parser.add_argument("--peer-id", required=True)
    announce_parser.add_argument("--base-url", required=True)

    peers_parser = sub.add_parser("peers", help="List tracker peers for a manifest.")
    peers_parser.add_argument("manifest")
    peers_parser.add_argument("--tracker", required=True)
    peers_parser.add_argument("--token", required=True)

    webui_parser = sub.add_parser("webui", help="Run the vendored AriaNg UI with a Bitswarm bridge.")
    webui_parser.add_argument("--host", default="127.0.0.1")
    webui_parser.add_argument("--port", type=int, default=8897)
    webui_parser.add_argument("--download-dir", default=None)
    webui_parser.add_argument(
        "--unsafe-allow-remote-bind",
        action="store_true",
        help="Allow binding the path-capable local UI to a non-loopback interface.",
    )

    args = parser.parse_args(argv)
    if args.command == "manifest":
        manifest = create_manifest(Path(args.path), piece_size=args.piece_size, name=args.name)
        save_manifest(manifest, Path(args.out))
        print(f"{manifest.manifest_id} {manifest.root_hash} {len(manifest.pieces)} pieces")
        return 0
    if args.command == "verify":
        manifest = load_manifest(Path(args.manifest))
        verify_manifest_tree(Path(args.path), manifest)
        print(f"verified {manifest.manifest_id}")
        return 0
    if args.command == "seed":
        manifest = load_manifest(Path(args.manifest)) if args.manifest else None
        if manifest is not None:
            verify_manifest_tree(Path(args.path), manifest)
        app = create_seeder_app(Path(args.path), manifest=manifest)
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    if args.command == "download":
        manifest = load_manifest(Path(args.manifest))
        peer_urls: list[str | PeerSource] = list(args.peer)
        if args.tracker:
            if not args.token:
                parser.error("--token is required with --tracker")
            peer_urls.extend(
                _tracker_peers(
                    tracker_url=args.tracker,
                    manifest_id=manifest.manifest_id,
                    token=args.token,
                    expected_piece_ids={piece.piece_id for piece in manifest.pieces},
                )
            )
        if args.tracker and not peer_urls:
            parser.error(f"tracker returned no peers for {manifest.manifest_id}")
        if not peer_urls:
            parser.error("download requires at least one peer source")

        async def progress(done: int, total: int, piece_id: str) -> None:
            print(f"{done}/{total} {piece_id}")

        asyncio.run(
            download_manifest(
                manifest,
                peer_urls=peer_urls,
                output_path=Path(args.out),
                progress_cb=progress,
            )
        )
        print(f"downloaded {manifest.manifest_id} -> {args.out}")
        return 0
    if args.command == "tracker":
        uvicorn.run(
            create_tracker_app(token=args.token, peer_ttl_ms=args.peer_ttl_ms),
            host=args.host,
            port=args.port,
        )
        return 0
    if args.command == "announce":
        manifest = load_manifest(Path(args.manifest))
        announcement = BitswarmAnnounce(
            peer_id=args.peer_id,
            base_url=args.base_url,
            manifest_id=manifest.manifest_id,
            piece_ids=full_piece_map(manifest),
        )
        base_url = str(announcement.base_url).rstrip("/")
        with httpx.Client(base_url=args.tracker, timeout=10.0) as client:
            response = client.post(
                "/api/announces",
                headers={**_auth_headers(args.token), "X-Bitswarm-Peer-Secret": args.peer_secret},
                json={**announcement.model_dump(mode="json"), "base_url": base_url},
            )
            response.raise_for_status()
        print(f"announced {args.peer_id} for {manifest.manifest_id}")
        return 0
    if args.command == "peers":
        manifest_id, expected_piece_ids = _manifest_identity_from_arg(args.manifest)
        if expected_piece_ids is None:
            parser.error("peers requires a manifest file path so tracker piece maps can be validated")
        for peer_source in _tracker_peers(
            tracker_url=args.tracker,
            manifest_id=manifest_id,
            token=args.token,
            expected_piece_ids=expected_piece_ids,
        ):
            print(peer_source.base_url if isinstance(peer_source, PeerSource) else peer_source)
        return 0
    if args.command == "webui":
        if not is_safe_local_bind(args.host) and not args.unsafe_allow_remote_bind:
            parser.error(
                "webui binds to loopback by default; pass --unsafe-allow-remote-bind explicitly"
            )
        uvicorn.run(
            create_ariang_app(
                default_output_dir=Path(args.download_dir).expanduser() if args.download_dir else None
            ),
            host=args.host,
            port=args.port,
        )
        return 0
    parser.error(f"unknown command {args.command}")
    return 2


def safe_main(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except (
        BitswarmError,
        FileNotFoundError,
        httpx.HTTPError,
        OSError,
        ValidationError,
        ValueError,
    ) as exc:
        print(f"bitswarm: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(safe_main())
