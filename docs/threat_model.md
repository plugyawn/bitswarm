# Threat Model

Bitswarm assumes peers may be buggy or malicious.

Defenses in `bitswarm-1.0-alpha.1`:

- public schemas reject unknown fields
- unsupported protocol IDs are rejected
- manifest IDs must be derived from the manifest root
- manifest paths use a conservative portable subset and cannot contain `:`, absolute roots, empty segments, `.`, `..`, repeated separators, or backslash aliases
- file-root and directory-root manifests are explicit and cannot alias each other
- empty directories are declared and verified
- symlink roots, symlink entries, hard-linked files, and special filesystem entries are rejected
- piece layouts must exactly cover declared files
- manifest creation and tree verification hash each declared file through one
  stable open file descriptor and reject files whose identity, size, mtime, or
  ctime changes while piece hashes are being computed
- manifest output files are written through no-follow parent traversal and
  existing symlink leaves are rejected
- every piece is hash-verified before write
- alpha manifests cap declared piece sizes at 16 MiB
- clients stop streaming a piece as soon as its response exceeds the declared
  piece size, before buffering the full oversized body
- seeders hash-check served piece bytes after request-time tree verification
  and recheck the served root identity before and after the piece read
- complete exact tree verification is required before promotion
- staging and promotion writes use descriptor-based no-follow traversal and
  reject hard-linked file aliases
- corrupted pieces raise an explicit verification error
- tracker announces require a bearer token and peer-specific secret
- tracker announces cap per-manifest piece IDs, per-peer manifest count, and
  control-plane ID length; control-plane IDs are restricted to URL-unreserved
  ASCII route-segment tokens
- tracker peer URLs must be HTTP(S) URLs with a fully-qualified domain or global
  IP literal; single-label, localhost, private, link-local, unspecified,
  reserved, and otherwise non-global IP targets are rejected in announces and
  tracker responses; username/password userinfo, path, query, and fragment URL
  components are rejected; resolvable DNS names must resolve only to globally
  routable addresses
- the reference downloader pins tracker-discovered hostnames to the validated
  global IP set used for the piece fetch; unresolved tracker hostnames fail
  closed at default download time
- clients intersect tracker piece maps with the loaded manifest before using
  tracker-discovered peers

Residual network-policy constraint:

- if DNS resolution fails locally during tracker peer URL validation, the alpha
  schema may accept the unresolved fully-qualified hostname; the default
  downloader will not connect without a validated IP address, but deployments
  that need strict egress control must still enforce resolver and IP filtering
  at their network boundary as well

Non-goals:

- anonymous public swarm operation
- payload encryption
- Byzantine consensus
- authority over application-specific state

Tracker authentication is bearer-token based with per-peer secret binding in the
alpha implementation. The tracker is still intended for trusted or
authenticated deployments, not unauthenticated public swarms.
Peer availability expires by TTL per `(peer_id, manifest_id)`, and each live
manifest availability keeps the endpoint from its own announce. Later announces
for other manifests do not refresh stale manifests or rewrite older live
manifest endpoints. Peer-secret bindings remain reserved for the life of the
in-memory tracker process to prevent expired-id takeover.
On macOS, top-level system compatibility symlinks such as `/tmp` and `/var` may
be traversed; user-controlled symlink components below that layer are rejected.
