# Integration Notes

Applications should use Bitswarm through a narrow adapter:

```text
input:
  manifest id
  trusted origin URL
  optional tracker URL
  optional auth token
  target cache/output path
  progress callback

output:
  verified local path
  manifest root
  pieces verified
  peer/source stats
```

Applications should keep their authority/control protocols separate from
Bitswarm. Bitswarm should only return verified bytes.

Bitswarm control-plane IDs are URL-unreserved ASCII route-segment tokens
matching `[A-Za-z0-9._~-]+`, capped at 128 characters. Applications should map
their own richer IDs to this transport-safe form before using tracker surfaces.

Tracker-discovered peer URLs are not equivalent to a caller-supplied trusted
origin URL. The alpha schemas accept only HTTP(S) peer URL origins with a
fully-qualified domain or global IP literal. Tracker peer URLs reject
username/password userinfo, path, query, fragment, single-label hosts,
localhost, private, link-local, unspecified, reserved, and otherwise non-global
IP literals in announces and tracker responses. When DNS resolution succeeds,
all resolved addresses must also be globally routable. Unresolved hostnames may
still be accepted so offline or private-control deployments can layer their own
resolver policy, but tracker-discovered URLs are revalidated before they are
listed or used. The reference downloader pins tracker-discovered hostnames to
the globally routable IP set observed during client-side validation. Hostnames
with no validated address fail closed in the default downloader. Applications
that supply their own HTTP client must explicitly opt out of the default
pinning guard and are then responsible for preserving equivalent
connection-time DNS pinning or stricter network policy.

Explicit direct peer URLs are trusted application inputs. They may point at
local or private origins for development or LAN deployment, but the reference
client still requires origin-only HTTP(S) URLs with no username/password
userinfo, path, query, or fragment. Tracker piece maps should be passed through
to the downloader so a bad or partial peer is not tried for every manifest
piece. Clients still verify every byte against the manifest, but applications
with stricter network policy should filter direct and tracker peers before
download.

## AriaNg UI Bridge

`bitswarm webui` serves a vendored build of the MIT-licensed AriaNg interface
from a local FastAPI app. AriaNg talks to `/jsonrpc` using aria2's JSON-RPC
shape; the local bridge maps the supported task/status calls to verified
Bitswarm manifest downloads.

The bridge is not a public transfer protocol. It is a local operator adapter
for existing torrent/download-manager UI expectations. Public Bitswarm peers
and trackers remain the endpoints documented in `docs/protocol.md`.

Supported add-URI forms include:

```text
bitswarm:?manifest=/absolute/path/manifest.json&peer=http%3A%2F%2F127.0.0.1%3A8899&out=/absolute/output/path
file:///absolute/path/manifest.json?peer=http%3A%2F%2F127.0.0.1%3A8899&out=/absolute/output/path
magnet:?xt=urn:bitswarm:<manifest-id>&xs=/absolute/path/manifest.json&x.pe=http%3A%2F%2F127.0.0.1%3A8899&x.out=/absolute/output/path
```

The bridge binds to loopback by default because it accepts local filesystem
paths. Remote binding requires `--unsafe-allow-remote-bind`.

### Shared Run Lobby

The AriaNg bridge includes a local shared run registry for all browser tabs
connected to one UI server. This is browser/product state only; it is not the
public Bitswarm transfer protocol.

Operators are represented as `A` through `O`. A tab can set its operator with a
query parameter such as `?actor=B`, or by using the operator dropdown in the Run
modal. The toolbar exposes Start Run and Runs controls in the same Bootstrap /
AriaNg chrome. Start Run lets the host choose a recipe, profile, visibility,
population, worker cap, and shortlist ratio. Runs lists active runs and lets
other operators join. A newly created run starts in `preparing` and exposes
torrent-style startup health checks before it goes green:

- downloading or verifying base weights
- connecting and confirming the deterministic seed manifest
- running an evaluator smoke check

These checks are local operator presentation state. External runtimes can report
real progress through the startup endpoint; the local bridge also drives a short
demo bootstrap so a standalone UI visibly moves from preparing to running.

Runs also expose an expandable seed view: seeds are sorted by `issued_at_ms`,
each seed can be expanded to its machine rollout table, and rollout rows use
normal Bootstrap success/danger/warning table states for correct, wrong/failed,
and pending/running rows. The registry is available through:

```text
GET  /api/bitswarm/ui/catalog
GET  /api/bitswarm/ui/runs
POST /api/bitswarm/ui/runs
POST /api/bitswarm/ui/runs/{run_id}/join
POST /api/bitswarm/ui/runs/{run_id}/startup/{stage_id}
POST /api/bitswarm/ui/runs/{run_id}/seeds/{seed_id}/rollouts
```

Registered runs are projected into aria2 JSON-RPC responses as normal active
tasks. AriaNg can therefore show running runs in the standard Downloading list,
with startup checks, members, settings, issued seeds, and rollout summaries
visible through normal task details. While a run is preparing, the projected
task progress is the aggregate startup check progress, so the stock AriaNg
progress bar behaves like torrent file checking. Once startup checks complete,
the task switches to running and shows member/join progress.

The UI can also render application progress such as training rounds, workers,
rollouts, validation, or other workload state through an optional sidecar
presentation feed:

```bash
uv run bitswarm webui --telemetry-json /path/to/telemetry.json
uv run bitswarm webui --telemetry-url http://127.0.0.1:9000/telemetry
```

That feed is consumed by the local `/api/bitswarm/ui/telemetry` endpoint and is
not part of the peer/tracker protocol. The bridge projects feed entries into
native aria2-style task rows, file rows, peers, and global stats so AriaNg keeps
its normal download-manager UI. Applications should write only presentation
state there, not authority records or optimizer/training control messages.

See `docs/examples/training-telemetry.json` for the concrete schema.
