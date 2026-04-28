# Bitswarm

Bitswarm is a verified peer-assisted transfer protocol for immutable portable
file trees.

The initial protocol target is `bitswarm-1.0`; this repository starts at
`bitswarm-1.0-alpha.1`.

Bitswarm is for moving portable byte trees safely:

- model artifacts
- dataset bundles
- snapshots
- generic immutable file trees

The alpha manifest format intentionally supports a conservative portable path
subset: normalized relative POSIX-style paths with no `:`, absolute roots,
empty segments, `.`, `..`, or backslash aliases. That excludes some valid local
POSIX filenames so manifests behave consistently across platforms.

Bitswarm is not an authority or training-control protocol. It does not carry
leases, proposal packets, replay reports, line-search records, commit records,
optimizer deltas, or application-specific authority.

## Quick Start

```bash
uv sync --extra dev
uv run bitswarm manifest ./some-file-tree --out manifest.json
uv run bitswarm verify ./some-file-tree manifest.json
uv run pytest
```

Serve a local immutable tree:

```bash
uv run bitswarm seed ./some-file-tree --host 127.0.0.1 --port 8899
```

Run a local tracker:

```bash
uv run bitswarm tracker --host 127.0.0.1 --port 8898 --token "$BITSWARM_TRACKER_TOKEN" --peer-ttl-ms 300000
```

Announce an externally reachable seeder and list peers through the tracker:

```bash
uv run bitswarm announce manifest.json --tracker http://127.0.0.1:8898 --token "$BITSWARM_TRACKER_TOKEN" \
  --peer-secret "$BITSWARM_PEER_SECRET" --peer-id public-peer --base-url https://peer.example
uv run bitswarm peers manifest.json --tracker http://127.0.0.1:8898 --token "$BITSWARM_TRACKER_TOKEN"
```

Tracker announces reject username/password userinfo, URL paths, query strings,
fragments, single-label hosts, localhost, private, link-local, and otherwise
non-global IP literals. Resolvable DNS names must resolve only to globally
routable addresses. If DNS resolution fails locally, the alpha schema may still
accept the hostname so deployments can apply their own resolver or egress
policy. Tracker-discovered URLs are revalidated before listing and download, and
the reference downloader pins them to the validated global IP set used for the
piece fetch. Tracker hostnames that have no validated address fail closed at
default download time. Advertised piece maps are preserved so partial peers are
only asked for pieces they advertised. For same-machine development, use the
direct `--peer` download path instead of tracker discovery.

Download from a local seeder with verification:

```bash
uv run bitswarm download manifest.json --peer http://127.0.0.1:8899 --out ./downloaded-tree
uv run bitswarm download manifest.json --tracker http://127.0.0.1:8898 --token "$BITSWARM_TRACKER_TOKEN" \
  --out ./downloaded-tree
```

## Protocol Contract

Every accepted byte must pass:

1. piece hash verification
2. manifest-root verification
3. exact piece coverage validation
4. exact root-shape verification
5. complete exact tree verification before cache promotion

Unknown fields are rejected on public protocol schemas.
Unsupported protocol IDs and manifest IDs not derived from the manifest root
are rejected.

## Development Gate

This repo is developed as a reusable protocol/runtime project. Every meaningful
change should be scoped, tested, documented when public behavior changes, and
reviewed before being marked complete.
