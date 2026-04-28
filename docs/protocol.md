# Bitswarm Protocol

Protocol id: `bitswarm-1.0-alpha.1`

Bitswarm transfers immutable file trees using strict manifests and verified
pieces. It is deliberately not an authority protocol.

## Public Objects

- `BitswarmManifest`
- `BitswarmDirectory`
- `BitswarmFile`
- `BitswarmPiece`
- `BitswarmPieceMap`
- `BitswarmPeer`
- `BitswarmAnnounce`
- `BitswarmRequest`
- `BitswarmResponse`
- `BitswarmVerification`

All public objects reject unknown fields.

## Manifest Root

The manifest root is SHA-256 over canonical JSON with sorted keys. The root
payload includes protocol id, root kind, piece size, total size, hash
algorithm, directories, files, and pieces. It excludes `name`, `manifest_id`,
and `root_hash`; `name` is display metadata and is not part of content
identity.

`manifest_id` is `bs-` plus the first 32 hex characters of the root hash.
Implementations must reject manifests whose `manifest_id` is not derived from
the root hash.

Canonical alpha manifests also require stable array order:

- `directories` sorted by path
- `files` sorted by path
- `pieces` sorted by `(file_path, offset)`
- `piece_id` values assigned sequentially as `p00000000`, `p00000001`, ...

Directory entries must declare their own parent directories, and file entries
must declare every parent directory. This keeps one file tree from having
multiple accepted manifest identities.

For alpha manifests, each `files[].sha256` is the SHA-256 digest of the
ordered piece-hash bytes for that file. Receivers verify actual bytes through
the piece range hashes. This keeps the manifest schema internally checkable:
file entries cannot contradict their piece list while still producing a new
canonical root hash.

File-root manifests use `.` as the canonical file path for their single
regular file. The receiver writes the payload to the requested output file path.

Only `bitswarm-1.0-alpha.1` is accepted by this alpha implementation.

Manifest files written by the reference CLI are emitted through no-follow parent
directory traversal. Existing symlink output leaves are rejected, and temporary
manifest writes are atomically renamed into place without following symlink
targets.

## Pieces

Pieces are file-local byte ranges:

```text
piece_id
file_path
offset
size
sha256
```

A receiver must verify each piece before writing it into staging.

Pieces must exactly cover every declared file. Gaps, overlaps, pieces that
reference undeclared files, and pieces that extend beyond their declared file
are invalid. Alpha manifests cap both `piece_size` and individual piece `size`
at 16 MiB so a valid manifest cannot require unbounded per-piece buffering.

Manifest paths are a portable subset, not the full native filesystem grammar:
normalized relative POSIX-style paths, no `:`, absolute roots, empty segments,
`.`, `..`, repeated separators, or backslash aliases.

## Root Shape

A manifest declares `root_kind` as either `file` or `directory`.

File-root manifests must be verified and downloaded as a single file. Directory
root manifests must be verified and downloaded as a directory, even when the
chosen output path has a suffix. Empty directories are represented explicitly in
`directories`, and undeclared directories are rejected during verification. The
only supported entries are unaliased regular files and directories. Symlink
roots, symlink entries, hard-linked files, sockets, FIFOs, devices, and other
special filesystem entries are rejected. On macOS, top-level system
compatibility symlinks such as `/tmp` and `/var` may be traversed so ordinary
temp/cache paths remain usable; any user-controlled symlink component beneath
that top-level compatibility layer is still rejected.

## Cache Promotion

The receiver writes verified pieces into a guarded staging directory using
descriptor-based no-follow traversal. The staging tree is copied into a fresh
promotion root from piece-verified bytes, fingerprinted, revalidated, and then
atomically installed through no-follow parent directory descriptors. The
destination is verified after install; if verification or install fails, the
previous destination is restored when possible and any unsafe replacement path is
removed rather than followed.
Verification is exact: undeclared files, symlinks, hard-linked files, or other
unexpected entries in the staged or promoted tree are rejected.

## Tracker

The alpha tracker requires a bearer token and a per-peer secret:

```text
Authorization: Bearer <tracker token>
X-Bitswarm-Peer-Secret: <peer secret>
```

The first announce binds a `peer_id` to its peer secret. Later announces for the
same `peer_id` must use the same secret. Tracker availability expires per
`(peer_id, manifest_id)` after the configured positive TTL, so refreshing one
manifest does not keep another manifest's piece map or peer endpoint alive.
Each manifest availability record carries the `base_url` from that manifest's
own announce, so a later announce for a different manifest cannot rewrite the
download endpoint returned for older live availability. Expiry removes live peer
availability, not the peer-secret binding. The in-memory alpha tracker keeps
peer-secret bindings for the life of the tracker process to prevent expired-id
takeover.

Tracker announces are bounded alpha control messages: at most 65,536 piece IDs
per manifest announce, at most 256 live manifests per peer, and at most 128
ASCII characters per control-plane ID. Control-plane IDs use the URL-unreserved
grammar `[A-Za-z0-9._~-]+` so they can round-trip as route path segments without
escaping. Longer IDs, larger piece lists, non-segment-safe IDs, or larger
per-peer manifest sets are rejected.

Tracker piece maps are availability hints, not manifest authority. Receivers
must intersect tracker-advertised piece IDs with the loaded manifest's declared
piece IDs before treating a peer as useful, and should retain that intersected
piece map while scheduling downloads. A tracker-discovered peer should only be
asked for a piece it advertised. Unknown piece IDs must not make a peer eligible
for download. Empty manifests may be announced with an empty piece map;
receivers should only use that empty map when their loaded manifest also
declares zero pieces.

Announced peer URLs are untrusted network inputs. The alpha schema accepts only
HTTP(S) URLs whose host is either a fully-qualified domain name or a global IP
literal. It rejects single-label hosts, localhost, private, link-local,
unspecified, reserved, and otherwise non-global IP targets in both tracker
announces and tracker peer responses. Tracker peer URLs must be origin-only:
username/password userinfo, path, query, and fragment components are rejected.
A deployment that wants private LAN peers or URL prefixes should use explicit
direct peer URLs or a separate trusted discovery layer rather than the public
tracker path. When DNS resolution succeeds during validation, every resolved
address must also be globally routable; unresolved hostnames may still be
accepted for deployments that provide their own resolver or egress policy.
Tracker stores and clients revalidate tracker-discovered URLs before surfacing
or using them. If a previously unresolved hostname later resolves to a local or
private address, the reference tracker omits that peer from listings rather than
returning a server error. The reference downloader also pins each
tracker-discovered hostname to the globally routable IP set observed during
client-side validation and connects through that pinned address set, while
preserving the original HTTP host and TLS server name. If no validated address
is available for a tracker-discovered hostname, the reference downloader fails
closed for that peer. Deployments with stricter egress policy should add their
own DNS/IP filtering before download.
