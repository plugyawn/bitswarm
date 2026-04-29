"""Local shared run registry for the AriaNg operator bridge.

This is UI/product coordination state for one web UI process. It is not part of
the public Bitswarm peer, tracker, or manifest protocol.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr

JsonScalar = StrictStr | StrictInt | StrictFloat | StrictBool

OPERATORS = tuple(chr(code) for code in range(ord("A"), ord("O") + 1))


class StrictUiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, validate_assignment=True)


class RunRecipe(StrictUiModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    description: StrictStr = ""
    model: StrictStr = Field(min_length=1)
    evaluator: StrictStr = Field(min_length=1)
    defaults: dict[str, JsonScalar] = Field(default_factory=dict)


class RunProfile(StrictUiModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    description: StrictStr = ""
    population: StrictInt = Field(gt=0)
    max_workers: StrictInt = Field(gt=0)
    shortlist_ratio: StrictFloat = Field(gt=0)


class RunMember(StrictUiModel):
    actor: StrictStr = Field(min_length=1)
    role: Literal["host", "worker"]
    state: Literal["hosting", "joined"] = "joined"
    joined_at_ms: StrictInt = Field(ge=0)


class RunRecord(StrictUiModel):
    run_id: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    recipe_id: StrictStr = Field(min_length=1)
    recipe_label: StrictStr = Field(min_length=1)
    profile_id: StrictStr = Field(min_length=1)
    profile_label: StrictStr = Field(min_length=1)
    host_actor: StrictStr = Field(min_length=1)
    visibility: Literal["public", "unlisted"] = "public"
    status: Literal["running", "paused", "complete", "error"] = "running"
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    members: list[RunMember] = Field(default_factory=list)
    settings: dict[str, JsonScalar] = Field(default_factory=dict)


class RunCreateRequest(StrictUiModel):
    actor: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1, max_length=120)
    recipe_id: StrictStr = Field(min_length=1)
    profile_id: StrictStr = Field(min_length=1)
    visibility: Literal["public", "unlisted"] = "public"
    settings: dict[str, JsonScalar] = Field(default_factory=dict)


class RunJoinRequest(StrictUiModel):
    actor: StrictStr = Field(min_length=1)


class RunCatalog(StrictUiModel):
    operators: list[StrictStr]
    recipes: list[RunRecipe]
    profiles: list[RunProfile]


class RunList(StrictUiModel):
    runs: list[RunRecord]


class RunRegistry:
    """In-process shared run registry for all browser tabs using one UI server."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._runs: dict[str, RunRecord] = {}
        self._recipes = _default_recipes()
        self._profiles = _default_profiles()

    async def catalog(self) -> RunCatalog:
        return RunCatalog(
            operators=list(OPERATORS),
            recipes=list(self._recipes.values()),
            profiles=list(self._profiles.values()),
        )

    async def list_runs(self) -> list[RunRecord]:
        async with self._lock:
            return sorted(self._runs.values(), key=lambda run: run.created_at_ms, reverse=True)

    async def create_run(self, request: RunCreateRequest) -> RunRecord:
        actor = _normalize_actor(request.actor)
        recipe = self._recipe(request.recipe_id)
        profile = self._profile(request.profile_id)
        now = _now_ms()
        settings: dict[str, JsonScalar] = {
            **recipe.defaults,
            "population": profile.population,
            "max_workers": profile.max_workers,
            "shortlist_ratio": profile.shortlist_ratio,
            **request.settings,
        }
        run = RunRecord(
            run_id=f"run-{secrets.token_hex(4)}",
            name=request.name.strip(),
            recipe_id=recipe.id,
            recipe_label=recipe.label,
            profile_id=profile.id,
            profile_label=profile.label,
            host_actor=actor,
            visibility=request.visibility,
            status="running",
            created_at_ms=now,
            updated_at_ms=now,
            members=[RunMember(actor=actor, role="host", state="hosting", joined_at_ms=now)],
            settings=settings,
        )
        async with self._lock:
            self._runs[run.run_id] = run
        return run

    async def join_run(self, run_id: str, request: RunJoinRequest) -> RunRecord:
        actor = _normalize_actor(request.actor)
        async with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise RunNotFound(run_id)
            now = _now_ms()
            members = [member for member in run.members if member.actor != actor]
            role: Literal["host", "worker"] = "host" if actor == run.host_actor else "worker"
            state: Literal["hosting", "joined"] = "hosting" if role == "host" else "joined"
            members.append(RunMember(actor=actor, role=role, state=state, joined_at_ms=now))
            updated = run.model_copy(update={"members": members, "updated_at_ms": now})
            self._runs[run_id] = updated
            return updated

    async def get_run(self, run_id: str) -> RunRecord | None:
        async with self._lock:
            return self._runs.get(run_id)

    def _recipe(self, recipe_id: str) -> RunRecipe:
        recipe = self._recipes.get(recipe_id)
        if recipe is None:
            raise RunConfigurationError(f"unknown recipe: {recipe_id}")
        return recipe

    def _profile(self, profile_id: str) -> RunProfile:
        profile = self._profiles.get(profile_id)
        if profile is None:
            raise RunConfigurationError(f"unknown profile: {profile_id}")
        return profile


class RunConfigurationError(Exception):
    """Raised when a run create request names an unknown recipe or profile."""


class RunNotFound(Exception):
    """Raised when a run id is not present in the local registry."""


def _default_recipes() -> dict[str, RunRecipe]:
    recipes = [
        RunRecipe(
            id="qwen05-arithmetic",
            label="Qwen 0.5B arithmetic",
            description="Local demo lane with a small arithmetic evaluation profile.",
            model="mlx-community/Qwen2.5-0.5B-Instruct-4bit",
            evaluator="lighteval:arithmetic_2da",
            defaults={"slice": "tail-4 + final RMSNorm", "paired_seeds": True},
        ),
        RunRecipe(
            id="qwen05-gsm8k-fast",
            label="Qwen 0.5B GSM8K Fast",
            description="Cached local testnet recipe for fast multi-worker smoke runs.",
            model="mlx-community/Qwen2.5-0.5B-Instruct-4bit",
            evaluator="lighteval:gsm8k_fast",
            defaults={"slice": "tail-4 + final RMSNorm", "paired_seeds": True},
        ),
        RunRecipe(
            id="qwen15-gsm8k",
            label="Qwen 1.5B GSM8K",
            description="Blessed lane recipe for the larger proposer/validator setup.",
            model="mlx-community/Qwen2.5-1.5B-Instruct-4bit",
            evaluator="lighteval:gsm8k",
            defaults={"slice": "tail-4 + final RMSNorm", "paired_seeds": True},
        ),
    ]
    return {recipe.id: recipe for recipe in recipes}


def _default_profiles() -> dict[str, RunProfile]:
    profiles = [
        RunProfile(
            id="smoke",
            label="Smoke",
            description="Tiny population for local UI and control-plane checks.",
            population=5,
            max_workers=14,
            shortlist_ratio=0.01,
        ),
        RunProfile(
            id="standard",
            label="Standard",
            description="Default operator run profile.",
            population=120,
            max_workers=14,
            shortlist_ratio=0.01,
        ),
        RunProfile(
            id="large",
            label="Large",
            description="Larger population for stronger distributed search.",
            population=512,
            max_workers=14,
            shortlist_ratio=0.01,
        ),
    ]
    return {profile.id: profile for profile in profiles}


def _normalize_actor(actor: str) -> str:
    normalized = actor.strip().upper()
    if normalized not in OPERATORS:
        raise RunConfigurationError(f"actor must be one of {', '.join(OPERATORS)}")
    return normalized


def _now_ms() -> int:
    return int(time.time() * 1000)
