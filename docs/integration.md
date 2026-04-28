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
