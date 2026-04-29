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

Run the vendored AriaNg Web UI with a local Bitswarm bridge:

```bash
uv run bitswarm webui --host 127.0.0.1 --port 8897
```

Open `http://127.0.0.1:8897`. The UI is the upstream MIT-licensed AriaNg
static app adapted through an aria2-compatible JSON-RPC bridge. To add a
Bitswarm transfer from AriaNg's New page, use a URI like:

```text
bitswarm:?manifest=/absolute/path/manifest.json&peer=http%3A%2F%2F127.0.0.1%3A8899&out=/absolute/output/path
```

The bridge also accepts a magnet-shaped Bitswarm URI, so existing
download-manager add flows can stay familiar:

```text
magnet:?xt=urn:bitswarm:<manifest-id>&xs=/absolute/path/manifest.json&x.pe=http%3A%2F%2F127.0.0.1%3A8899&x.out=/absolute/output/path
```

The UI bridge is local operator tooling. It does not change the public Bitswarm
peer or tracker protocol.

The same bridge also exposes a shared local run lobby for browser tabs pointed
at the same UI server. Operators identify themselves as `A` through `O` with
`?actor=A` or the operator dropdown. One operator can open Start Run, choose a
recipe/profile, and start a run; other operators refresh or open Runs and join
that run. Newly started runs first show torrent-style startup health: base
weights download/verification, seed manifest confirmation, and evaluator smoke
validation. Those startup checks drive the normal AriaNg progress bar until the
run goes green and switches to running. The Runs view expands each run into
issued seeds sorted by issue time, and each seed expands into pending/completed
rollout rows per machine with green/red Bootstrap table states for correct and
wrong results. Run state is also projected into AriaNg as ordinary active task
rows, so the running run appears in the normal download-manager list.

Applications can project training or other workload progress into the UI with a
separate sidecar presentation feed:

```bash
uv run bitswarm webui --telemetry-json /path/to/telemetry.json
```

That feed is local UI state only; it is not carried by Bitswarm peers or
trackers. The AriaNg bridge presents sidecar workload progress as ordinary
aria2 task rows, file rows, peer rows, and global stats. There is no separate
custom dashboard layered on top of AriaNg.

A runnable sample lives at `docs/examples/training-telemetry.json`.

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
