"""Typed local presentation telemetry for the AriaNg bridge.

This is intentionally a UI sidecar contract. It is not part of the Bitswarm
peer, tracker, or manifest protocol.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr


class StrictTelemetryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, validate_assignment=True)


class TelemetryMetric(StrictTelemetryModel):
    label: StrictStr = Field(min_length=1)
    value: StrictStr = Field(min_length=1)
    detail: StrictStr = ""


class TelemetryProgress(StrictTelemetryModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    state: StrictStr = Field(min_length=1)
    current: StrictFloat | StrictInt = Field(ge=0)
    total: StrictFloat | StrictInt = Field(gt=0)
    unit: StrictStr = "items"
    detail: StrictStr = ""
    rate: StrictStr = ""


class TelemetryMember(StrictTelemetryModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    role: StrictStr = ""
    state: StrictStr = Field(min_length=1)
    detail: StrictStr = ""
    current: StrictFloat | StrictInt | None = Field(default=None, ge=0)
    total: StrictFloat | StrictInt | None = Field(default=None, gt=0)


class TelemetryStream(StrictTelemetryModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    kind: StrictStr = ""
    state: StrictStr = Field(min_length=1)
    current: StrictFloat | StrictInt | None = Field(default=None, ge=0)
    total: StrictFloat | StrictInt | None = Field(default=None, gt=0)
    prompt: StrictStr = ""
    output: StrictStr = ""
    score: StrictStr = ""
    detail: StrictStr = ""


class TelemetryEvent(StrictTelemetryModel):
    ts_ms: StrictInt = Field(ge=0)
    level: Literal["debug", "info", "warning", "error"] = "info"
    message: StrictStr = Field(min_length=1)


class WorkloadTelemetry(StrictTelemetryModel):
    enabled: bool = False
    title: StrictStr = "Bitswarm"
    subtitle: StrictStr = "No workload sidecar connected."
    workload_type: StrictStr = "transfer"
    status: StrictStr = "idle"
    phase: StrictStr = "idle"
    updated_at_ms: StrictInt = Field(default_factory=lambda: int(time.time() * 1000), ge=0)
    metrics: list[TelemetryMetric] = Field(default_factory=list)
    progress: list[TelemetryProgress] = Field(default_factory=list)
    members: list[TelemetryMember] = Field(default_factory=list)
    streams: list[TelemetryStream] = Field(default_factory=list)
    events: list[TelemetryEvent] = Field(default_factory=list)


class TelemetryProvider:
    """Load local operator telemetry from an optional JSON file or HTTP endpoint."""

    def __init__(self, *, json_path: Path | None = None, url: str | None = None) -> None:
        if json_path is not None and url is not None:
            raise ValueError("configure either telemetry JSON path or telemetry URL, not both")
        self._json_path = json_path
        self._url = url

    async def snapshot(self) -> WorkloadTelemetry:
        if self._json_path is None and self._url is None:
            return WorkloadTelemetry()
        if self._json_path is not None:
            payload = json.loads(self._json_path.expanduser().read_text(encoding="utf-8"))
            return WorkloadTelemetry.model_validate(payload)
        assert self._url is not None
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(self._url)
            response.raise_for_status()
            return WorkloadTelemetry.model_validate(response.json())
