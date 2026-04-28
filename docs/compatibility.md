# Compatibility Policy

The alpha protocol id is `bitswarm-1.0-alpha.1`.

Compatibility rules before `bitswarm-1.0`:

- breaking schema changes are allowed only with a protocol id bump
- fixture tests should pin canonical manifest behavior
- public schemas must reject unknown fields
- clients must fail explicitly on unsupported protocol ids
- manifests must reject IDs not derived from their root hash
- piece layouts must reject gaps and overlaps

Compatibility rules for `bitswarm-1.0` will be stricter:

- no breaking changes within the `1.x` major line
- additive fields require documented default behavior
- incompatible peers must receive explicit unsupported-protocol errors
