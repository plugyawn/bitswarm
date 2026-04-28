"""HTTP transport helpers for downloader peer safety."""

from __future__ import annotations

import ssl
from collections.abc import AsyncIterable, Mapping, Sequence
from types import TracebackType

import httpcore
import httpx
from httpcore._backends.auto import AutoBackend
from httpcore._backends.base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream
from httpx._transports.default import (  # type: ignore[attr-defined]
    DEFAULT_LIMITS,
    AsyncResponseStream,
    create_ssl_context,
    map_httpcore_exceptions,
)
from httpx._types import CertTypes


class PinnedDNSAsyncNetworkBackend(AsyncNetworkBackend):
    """Network backend that connects selected hostnames to validated IPs."""

    def __init__(
        self,
        pins: Mapping[str, Sequence[str]],
        *,
        delegate: AsyncNetworkBackend | None = None,
    ) -> None:
        self._pins = {host: tuple(ips) for host, ips in pins.items()}
        self._delegate = delegate or AutoBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Sequence[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        normalized_host = _normalize_transport_host(host)
        pinned_ips = self._pins.get(normalized_host)
        if pinned_ips is not None:
            if not pinned_ips:
                raise httpcore.ConnectError(
                    f"tracker-discovered peer {normalized_host} has no validated DNS address"
                )
            host = pinned_ips[0]
        return await self._delegate.connect_tcp(
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Sequence[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        return await self._delegate.connect_unix_socket(path, timeout=timeout, socket_options=socket_options)

    async def sleep(self, seconds: float) -> None:
        await self._delegate.sleep(seconds)


class PinnedDNSAsyncHTTPTransport(httpx.AsyncBaseTransport):
    """HTTP transport that pins selected peer hostnames below httpcore."""

    def __init__(
        self,
        pins: Mapping[str, Sequence[str]],
        *,
        verify: ssl.SSLContext | str | bool = True,
        cert: CertTypes | None = None,
        trust_env: bool = True,
        limits: httpx.Limits = DEFAULT_LIMITS,
    ) -> None:
        ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env)
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl_context,
            max_connections=limits.max_connections,
            max_keepalive_connections=limits.max_keepalive_connections,
            keepalive_expiry=limits.keepalive_expiry,
            http1=True,
            http2=False,
            network_backend=PinnedDNSAsyncNetworkBackend(pins),
        )

    async def __aenter__(self) -> PinnedDNSAsyncHTTPTransport:
        await self._pool.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        with map_httpcore_exceptions():
            await self._pool.__aexit__(exc_type, exc_value, traceback)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)
        req = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        with map_httpcore_exceptions():
            response = await self._pool.handle_async_request(req)
        assert isinstance(response.stream, AsyncIterable)
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=AsyncResponseStream(response.stream),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()


def _normalize_transport_host(host: str | bytes) -> str:
    if isinstance(host, bytes):
        host = host.decode("ascii")
    return host.strip("[]").rstrip(".").lower()
