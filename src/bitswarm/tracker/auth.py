"""Tracker authentication helpers."""

from __future__ import annotations

from fastapi import Header, HTTPException

PEER_SECRET_HEADER = "X-Bitswarm-Peer-Secret"


def validate_bearer_token(authorization: str | None, *, expected_token: str | None) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != expected_token:
        raise HTTPException(status_code=403, detail="invalid bearer token")


async def auth_header(authorization: str | None = Header(default=None)) -> str | None:
    return authorization


async def peer_secret_header(
    peer_secret: str | None = Header(default=None, alias=PEER_SECRET_HEADER),
) -> str | None:
    return peer_secret
