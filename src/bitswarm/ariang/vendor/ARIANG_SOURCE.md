# AriaNg Vendor Source

Bitswarm vendors the built AriaNg static UI for the local operator console.

- Upstream: https://github.com/mayswind/AriaNg
- Upstream commit: `7ad711a4f1e66b8fc59fe0127c97b588cc908ed4`
- Upstream license: MIT, copied in `ARIANG_LICENSE`
- Build command: `npm install && npm run build`

Local adaptation:

- The built AriaNg default JSON-RPC port is patched from `6800` to the current
  page port so a Bitswarm-served UI talks to its co-located `/jsonrpc` bridge.
- The built AriaNg local-storage prefix is patched from `AriaNg` to
  `BitswarmAriaNg` so this bridge does not inherit unrelated user settings from
  a normal AriaNg install.
- Visible first-run chrome is patched to say Bitswarm, route first-run users to
  the download list instead of stock AriaNg settings, and hide the irrelevant
  Aria2 settings/status routes behind `bitswarm-adapter.js`/`.css`.
- `bitswarm-adapter.js` does not render custom workload panels. Workload
  telemetry is projected through the local aria2-compatible JSON-RPC bridge as
  normal AriaNg task/file/peer/status rows.
- The public Bitswarm transfer protocol is not changed by this UI. The bridge
  emulates the subset of aria2 JSON-RPC that AriaNg needs and maps those calls
  onto verified Bitswarm manifest downloads.
