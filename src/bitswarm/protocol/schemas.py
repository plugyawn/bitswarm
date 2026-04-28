"""Strict public protocol schemas for Bitswarm."""

from __future__ import annotations

import hashlib
import ipaddress
import socket
from typing import Annotated, Literal

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    TypeAdapter,
    field_validator,
    model_validator,
)

from bitswarm.constants import (
    CONTROL_ID_PATTERN,
    FILE_ROOT_PATH,
    MAX_ID_LENGTH,
    MAX_PIECE_SIZE,
    MAX_TRACKER_MANIFESTS_PER_PEER,
    MAX_TRACKER_PIECES_PER_ANNOUNCE,
    PIECE_HASH_ALGORITHM,
    PROTOCOL_ID,
)

from .hashing import manifest_root

StrictModelConfig = ConfigDict(extra="forbid", strict=True, validate_assignment=True)
ProtocolId = Literal["bitswarm-1.0-alpha.1"]
RootKind = Literal["file", "directory"]
Sha256Hex = str
_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)
ControlId = Annotated[
    str,
    Field(min_length=1, max_length=MAX_ID_LENGTH, pattern=CONTROL_ID_PATTERN),
]


class StrictModel(BaseModel):
    model_config = StrictModelConfig


def _normalize_relative_path(value: str) -> str:
    if value == FILE_ROOT_PATH:
        return value
    if "\\" in value:
        raise ValueError("path must be a normalized relative file path")
    normalized = value
    parts = normalized.split("/")
    if normalized.startswith("/") or ":" in normalized or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("path must be a normalized relative file path")
    return normalized


class BitswarmFile(StrictModel):
    path: str = Field(min_length=1)
    size: StrictInt = Field(ge=0)
    sha256: Sha256Hex = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("path")
    @classmethod
    def reject_absolute_or_parent_paths(cls, value: str) -> str:
        return _normalize_relative_path(value)


class BitswarmDirectory(StrictModel):
    path: str = Field(min_length=1)

    @field_validator("path")
    @classmethod
    def reject_absolute_or_parent_paths(cls, value: str) -> str:
        return _normalize_relative_path(value)


class BitswarmPiece(StrictModel):
    piece_id: ControlId
    file_path: str = Field(min_length=1)
    offset: StrictInt = Field(ge=0)
    size: StrictInt = Field(gt=0, le=MAX_PIECE_SIZE)
    sha256: Sha256Hex = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("file_path")
    @classmethod
    def reject_absolute_or_parent_paths(cls, value: str) -> str:
        return _normalize_relative_path(value)


class BitswarmManifest(StrictModel):
    protocol_id: ProtocolId = PROTOCOL_ID
    manifest_id: ControlId
    root_hash: Sha256Hex = Field(pattern=r"^[0-9a-f]{64}$")
    name: str = Field(min_length=1)
    root_kind: RootKind
    piece_size: StrictInt = Field(gt=0, le=MAX_PIECE_SIZE)
    total_size: StrictInt = Field(ge=0)
    hash_algorithm: Literal["sha256"] = PIECE_HASH_ALGORITHM
    directories: list[BitswarmDirectory] = Field(default_factory=list)
    files: list[BitswarmFile] = Field(default_factory=list)
    pieces: list[BitswarmPiece] = Field(default_factory=list)

    @field_validator("directories")
    @classmethod
    def unique_directory_paths(cls, value: list[BitswarmDirectory]) -> list[BitswarmDirectory]:
        paths = [item.path for item in value]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest contains duplicate directory paths")
        return value

    @field_validator("files")
    @classmethod
    def unique_file_paths(cls, value: list[BitswarmFile]) -> list[BitswarmFile]:
        paths = [item.path for item in value]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest contains duplicate file paths")
        return value

    @field_validator("pieces")
    @classmethod
    def unique_piece_ids(cls, value: list[BitswarmPiece]) -> list[BitswarmPiece]:
        ids = [item.piece_id for item in value]
        if len(ids) != len(set(ids)):
            raise ValueError("manifest contains duplicate piece ids")
        return value

    @model_validator(mode="after")
    def pieces_reference_declared_files(self) -> BitswarmManifest:
        if self.root_kind == "file" and len(self.files) != 1:
            raise ValueError("file-root manifests must contain exactly one file")
        if self.root_kind == "file" and self.directories:
            raise ValueError("file-root manifests must not declare directories")
        if self.root_kind == "file" and self.files and self.files[0].path != FILE_ROOT_PATH:
            raise ValueError(f"file-root manifests must use {FILE_ROOT_PATH!r} as the file path")
        if self.root_kind == "directory":
            if FILE_ROOT_PATH in {item.path for item in self.directories}:
                raise ValueError(
                    f"directory-root manifests must not use {FILE_ROOT_PATH!r} as a directory path"
                )
            if FILE_ROOT_PATH in {item.path for item in self.files}:
                raise ValueError(f"directory-root manifests must not use {FILE_ROOT_PATH!r} as a file path")
        directory_list = [item.path for item in self.directories]
        if directory_list != sorted(directory_list):
            raise ValueError("directories must be sorted by path")
        file_list = [item.path for item in self.files]
        if file_list != sorted(file_list):
            raise ValueError("files must be sorted by path")
        piece_order = [(item.file_path, item.offset) for item in self.pieces]
        if piece_order != sorted(piece_order):
            raise ValueError("pieces must be sorted by file path and offset")
        for index, piece in enumerate(self.pieces):
            expected_piece_id = f"p{index:08d}"
            if piece.piece_id != expected_piece_id:
                raise ValueError(f"piece ids must be canonical and sequential; expected {expected_piece_id}")
        files_by_path = {item.path: item for item in self.files}
        directory_paths = {item.path for item in self.directories}
        if directory_paths.intersection(files_by_path):
            raise ValueError("a manifest path cannot be both a file and a directory")
        for directory_path in directory_paths:
            parts = directory_path.split("/")[:-1]
            for index in range(1, len(parts) + 1):
                parent = "/".join(parts[:index])
                if parent not in directory_paths:
                    raise ValueError(f"directory {directory_path} parent directory {parent} is not declared")
        for file_path in files_by_path:
            parts = file_path.split("/")[:-1]
            for index in range(1, len(parts) + 1):
                parent = "/".join(parts[:index])
                if parent not in directory_paths:
                    raise ValueError(f"file {file_path} parent directory {parent} is not declared")
        if sum(item.size for item in self.files) != self.total_size:
            raise ValueError("total_size must equal the sum of declared file sizes")
        pieces_by_file: dict[str, list[BitswarmPiece]] = {path: [] for path in files_by_path}
        for piece in self.pieces:
            file = files_by_path.get(piece.file_path)
            if file is None:
                raise ValueError(f"piece {piece.piece_id} references undeclared file {piece.file_path}")
            if piece.offset + piece.size > file.size:
                raise ValueError(f"piece {piece.piece_id} exceeds file size for {piece.file_path}")
            pieces_by_file[piece.file_path].append(piece)
        for file in self.files:
            file_digest = hashlib.sha256()
            ranges = []
            for piece in sorted(pieces_by_file[file.path], key=lambda item: item.offset):
                ranges.append((piece.offset, piece.offset + piece.size))
                if piece.size > 0:
                    file_digest.update(bytes.fromhex(piece.sha256))
            expected = 0
            for index, (start, end) in enumerate(ranges):
                if start != expected:
                    raise ValueError(f"pieces for {file.path} do not fully cover the file")
                piece_size = end - start
                is_final_piece = index == len(ranges) - 1
                if not is_final_piece and piece_size != self.piece_size:
                    raise ValueError(f"pieces for {file.path} must use canonical piece_size chunks")
                if is_final_piece and piece_size > self.piece_size:
                    raise ValueError(f"final piece for {file.path} exceeds piece_size")
                expected = end
            if expected != file.size:
                raise ValueError(f"pieces for {file.path} do not fully cover the file")
            expected_file_hash = file_digest.hexdigest()
            if file.sha256 != expected_file_hash:
                raise ValueError(f"file sha256 must match canonical digest of piece hashes for {file.path}")
        expected_manifest_id = f"bs-{self.root_hash[:32]}"
        if self.manifest_id != expected_manifest_id:
            raise ValueError(f"manifest_id must be derived from root_hash as {expected_manifest_id}")
        canonical_payload = {
            "protocol_id": self.protocol_id,
            "root_kind": self.root_kind,
            "piece_size": self.piece_size,
            "total_size": self.total_size,
            "hash_algorithm": self.hash_algorithm,
            "directories": [directory.model_dump(mode="json") for directory in self.directories],
            "files": [file.model_dump(mode="json") for file in self.files],
            "pieces": [piece.model_dump(mode="json") for piece in self.pieces],
        }
        expected_root_hash = manifest_root(canonical_payload)
        if self.root_hash != expected_root_hash:
            raise ValueError("root_hash must match the canonical manifest payload")
        return self


class BitswarmPieceMap(StrictModel):
    manifest_id: ControlId
    piece_ids: list[ControlId] = Field(
        default_factory=list,
        max_length=MAX_TRACKER_PIECES_PER_ANNOUNCE,
    )


class BitswarmPeer(StrictModel):
    peer_id: ControlId
    base_url: AnyHttpUrl
    manifests: list[ControlId] = Field(
        default_factory=list,
        max_length=MAX_TRACKER_MANIFESTS_PER_PEER,
    )
    updated_at_ms: StrictInt = Field(ge=0)

    @field_validator("base_url")
    @classmethod
    def reject_local_peer_urls(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        return _validate_peer_url(value, allow_private=False)


class BitswarmAnnounce(StrictModel):
    peer_id: ControlId
    base_url: AnyHttpUrl
    manifest_id: ControlId
    piece_ids: list[ControlId] = Field(
        default_factory=list,
        max_length=MAX_TRACKER_PIECES_PER_ANNOUNCE,
    )

    @field_validator("base_url")
    @classmethod
    def reject_local_peer_urls(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        return _validate_peer_url(value, allow_private=False)


class BitswarmRequest(StrictModel):
    manifest_id: ControlId
    piece_id: ControlId


class BitswarmResponse(StrictModel):
    manifest_id: ControlId
    piece_id: ControlId
    size: StrictInt = Field(ge=0)
    sha256: Sha256Hex = Field(pattern=r"^[0-9a-f]{64}$")


class BitswarmVerification(StrictModel):
    manifest_id: ControlId
    verified: bool
    pieces_verified: StrictInt = Field(ge=0)
    total_pieces: StrictInt = Field(ge=0)
    message: str = ""


def validate_peer_base_url(value: str, *, allow_private: bool = False) -> str:
    """Validate and normalize a peer origin URL.

    Tracker-originated peer URLs must be globally routable. Caller-supplied
    direct peers may opt into local/private origins for explicit trusted
    development or LAN use, but still must be origin-only and credential-free.
    """
    base_url, _, _ = validate_peer_base_url_with_dns(value, allow_private=allow_private)
    return base_url


def validate_peer_base_url_with_dns(
    value: str,
    *,
    allow_private: bool = False,
) -> tuple[str, str, frozenset[str]]:
    """Validate a peer origin URL and return the host plus validated IP set.

    The returned IP set is empty for explicit direct peers and for public FQDNs
    that did not resolve during validation.
    """
    parsed = _HTTP_URL_ADAPTER.validate_python(value)
    checked = _validate_peer_url(parsed, allow_private=allow_private)
    host = _normalized_peer_host(checked)
    if allow_private:
        return str(checked).rstrip("/"), host, frozenset()
    pins = _validated_public_peer_ips(host)
    return str(checked).rstrip("/"), host, frozenset(pins)


def _validate_peer_url(value: AnyHttpUrl, *, allow_private: bool) -> AnyHttpUrl:
    if value.username or value.password:
        raise ValueError("base_url must not include username or password")
    if value.path not in {"", "/"} or value.query or value.fragment:
        raise ValueError("base_url must not include path, query, or fragment")
    host = _normalized_peer_host(value)
    if allow_private:
        return value
    _validated_public_peer_ips(host)
    return value


def _normalized_peer_host(value: AnyHttpUrl) -> str:
    host = value.host
    if host is None:
        raise ValueError("base_url must include a host")
    return host.strip("[]").rstrip(".").lower()


def _validated_public_peer_ips(host: str) -> list[str]:
    if host == "localhost" or host.endswith(".localhost"):
        raise ValueError("base_url must not target local or private addresses")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host:
            raise ValueError("base_url host must be a public fully-qualified domain or global IP") from None
        pins: list[str] = []
        for resolved_ip in _resolved_host_ips(host):
            if isinstance(resolved_ip, ipaddress.IPv6Address) and resolved_ip.ipv4_mapped is not None:
                resolved_ip = resolved_ip.ipv4_mapped
            if not resolved_ip.is_global:
                raise ValueError(
                    "base_url DNS resolution must not target local or private addresses"
                ) from None
            pins.append(str(resolved_ip))
        return pins
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if not ip.is_global:
        raise ValueError("base_url must not target local or private addresses")
    return [str(ip)]


def _resolved_host_ips(host: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        records = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return []
    resolved: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[str] = set()
    for record in records:
        sockaddr = record[4]
        if not sockaddr:
            continue
        ip_text = str(sockaddr[0]).split("%", 1)[0]
        try:
            ip = ipaddress.ip_address(ip_text)
        except ValueError:
            continue
        key = str(ip)
        if key not in seen:
            resolved.append(ip)
            seen.add(key)
    return resolved
