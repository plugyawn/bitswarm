"""Protocol errors."""


class BitswarmError(Exception):
    """Base Bitswarm error."""


class ManifestError(BitswarmError):
    """Raised for invalid manifests."""


class PieceVerificationError(BitswarmError):
    """Raised when a piece fails verification."""


class PieceUnavailableError(BitswarmError):
    """Raised when a piece cannot be fetched from any peer."""


class TreeVerificationError(BitswarmError):
    """Raised when a file tree fails manifest verification."""


class CachePromotionError(BitswarmError):
    """Raised when verified data cannot be promoted into a cache/output path."""
