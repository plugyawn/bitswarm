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


class StartupCheck(StrictUiModel):
    id: StrictStr = Field(min_length=1)
    label: StrictStr = Field(min_length=1)
    state: Literal["pending", "running", "complete", "failed"] = "pending"
    current: StrictInt = Field(ge=0)
    total: StrictInt = Field(gt=0)
    detail: StrictStr = ""
    started_at_ms: StrictInt | None = Field(default=None, ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    completed_at_ms: StrictInt | None = Field(default=None, ge=0)


class RolloutRecord(StrictUiModel):
    rollout_id: StrictStr = Field(min_length=1)
    seed_id: StrictStr = Field(min_length=1)
    machine: StrictStr = Field(min_length=1)
    item_id: StrictStr = Field(min_length=1)
    sign: Literal["+", "-"] = "+"
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    issued_at_ms: StrictInt = Field(ge=0)
    completed_at_ms: StrictInt | None = Field(default=None, ge=0)
    correct: StrictBool | None = None
    score: StrictFloat | None = None
    expected: StrictStr = ""
    output: StrictStr = ""


class SeedRecord(StrictUiModel):
    seed_id: StrictStr = Field(min_length=1)
    sigma_id: StrictStr = Field(min_length=1)
    issued_at_ms: StrictInt = Field(ge=0)
    state: Literal["pending", "leased", "completed"] = "pending"
    rollouts: list[RolloutRecord] = Field(default_factory=list)


class RunRecord(StrictUiModel):
    run_id: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    recipe_id: StrictStr = Field(min_length=1)
    recipe_label: StrictStr = Field(min_length=1)
    profile_id: StrictStr = Field(min_length=1)
    profile_label: StrictStr = Field(min_length=1)
    host_actor: StrictStr = Field(min_length=1)
    visibility: Literal["public", "unlisted"] = "public"
    status: Literal["preparing", "running", "paused", "complete", "error"] = "preparing"
    created_at_ms: StrictInt = Field(ge=0)
    updated_at_ms: StrictInt = Field(ge=0)
    startup_checks: list[StartupCheck] = Field(default_factory=list)
    members: list[RunMember] = Field(default_factory=list)
    seeds: list[SeedRecord] = Field(default_factory=list)
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


class RolloutUpdateRequest(StrictUiModel):
    machine: StrictStr = Field(min_length=1)
    item_id: StrictStr = Field(min_length=1)
    sign: Literal["+", "-"] = "+"
    status: Literal["pending", "running", "completed", "failed"]
    correct: StrictBool | None = None
    score: StrictFloat | None = None
    expected: StrictStr = ""
    output: StrictStr = ""


class StartupCheckUpdateRequest(StrictUiModel):
    state: Literal["pending", "running", "complete", "failed"] | None = None
    current: StrictInt | None = Field(default=None, ge=0)
    total: StrictInt | None = Field(default=None, gt=0)
    detail: StrictStr | None = None


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
        settings = _normalize_run_settings(settings=settings, profile=profile)
        run = RunRecord(
            run_id=f"run-{secrets.token_hex(4)}",
            name=request.name.strip(),
            recipe_id=recipe.id,
            recipe_label=recipe.label,
            profile_id=profile.id,
            profile_label=profile.label,
            host_actor=actor,
            visibility=request.visibility,
            status="preparing",
            created_at_ms=now,
            updated_at_ms=now,
            startup_checks=_make_startup_checks(
                recipe=recipe,
                population=int(settings.get("population", profile.population)),
                now_ms=now,
            ),
            members=[RunMember(actor=actor, role="host", state="hosting", joined_at_ms=now)],
            seeds=_make_seed_records(
                population=int(settings.get("population", profile.population)),
                now_ms=now,
            ),
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

    async def update_rollout(
        self,
        run_id: str,
        seed_id: str,
        request: RolloutUpdateRequest,
    ) -> RunRecord:
        async with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise RunNotFound(run_id)
            now = _now_ms()
            seeds: list[SeedRecord] = []
            found_seed = False
            for seed in run.seeds:
                if seed.seed_id != seed_id:
                    seeds.append(seed)
                    continue
                found_seed = True
                rollout_id = f"{seed_id}:{request.sign}:{request.machine}:{request.item_id}"
                existing = [row for row in seed.rollouts if row.rollout_id != rollout_id]
                previous = next((row for row in seed.rollouts if row.rollout_id == rollout_id), None)
                rollout = RolloutRecord(
                    rollout_id=rollout_id,
                    seed_id=seed_id,
                    machine=request.machine,
                    item_id=request.item_id,
                    sign=request.sign,
                    status=request.status,
                    issued_at_ms=previous.issued_at_ms if previous is not None else now,
                    completed_at_ms=now if request.status in {"completed", "failed"} else None,
                    correct=request.correct,
                    score=request.score,
                    expected=request.expected,
                    output=request.output,
                )
                rollouts = sorted([*existing, rollout], key=lambda row: (row.issued_at_ms, row.rollout_id))
                state = _seed_state(rollouts)
                seeds.append(seed.model_copy(update={"rollouts": rollouts, "state": state}))
            if not found_seed:
                raise RunConfigurationError(f"unknown seed: {seed_id}")
            updated = run.model_copy(update={"seeds": seeds, "updated_at_ms": now})
            self._runs[run_id] = updated
            return updated

    async def update_startup_check(
        self,
        run_id: str,
        stage_id: str,
        request: StartupCheckUpdateRequest,
    ) -> RunRecord:
        async with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise RunNotFound(run_id)
            now = _now_ms()
            checks: list[StartupCheck] = []
            found = False
            for check in run.startup_checks:
                if check.id != stage_id:
                    checks.append(check)
                    continue
                found = True
                state = request.state or check.state
                total = request.total if request.total is not None else check.total
                current = request.current if request.current is not None else check.current
                if current > total:
                    raise RunConfigurationError(
                        f"startup check {stage_id} current exceeds total: {current}>{total}"
                    )
                started_at_ms = check.started_at_ms
                if state == "running" and started_at_ms is None:
                    started_at_ms = now
                completed_at_ms = check.completed_at_ms
                if state in {"complete", "failed"}:
                    completed_at_ms = now
                elif check.state in {"complete", "failed"} and state not in {"complete", "failed"}:
                    completed_at_ms = None
                checks.append(
                    check.model_copy(
                        update={
                            "state": state,
                            "current": current,
                            "total": total,
                            "detail": request.detail if request.detail is not None else check.detail,
                            "started_at_ms": started_at_ms,
                            "updated_at_ms": now,
                            "completed_at_ms": completed_at_ms,
                        }
                    )
                )
            if not found:
                raise RunConfigurationError(f"unknown startup check: {stage_id}")
            status = _run_status_from_startup(checks, run.status)
            updated = run.model_copy(
                update={"startup_checks": checks, "status": status, "updated_at_ms": now}
            )
            self._runs[run_id] = updated
            return updated

    async def bootstrap_run(self, run_id: str, *, delay_seconds: float = 0.35) -> RunRecord | None:
        """Drive the local UI bridge startup checks for a newly created run.

        This is local operator bridge state, not public Bitswarm protocol state.
        External runtimes can instead call ``update_startup_check`` directly.
        """

        run = await self.get_run(run_id)
        if run is None:
            return None
        population = max(1, int(run.settings.get("population", len(run.seeds) or 1)))
        await self.update_startup_check(
            run_id,
            "base-weights",
            StartupCheckUpdateRequest(
                state="running",
                current=0,
                detail=f"checking {run.recipe_label} model files",
            ),
        )
        for current in (20, 45, 70, 100):
            await asyncio.sleep(delay_seconds)
            await self.update_startup_check(
                run_id,
                "base-weights",
                StartupCheckUpdateRequest(
                    state="complete" if current == 100 else "running",
                    current=current,
                    detail=(
                        "base weights present and hashable"
                        if current == 100
                        else "checking cached model shards"
                    ),
                ),
            )
        await self.update_startup_check(
            run_id,
            "seed-handshake",
            StartupCheckUpdateRequest(
                state="running",
                current=0,
                detail="announcing deterministic seed manifest",
            ),
        )
        step = max(1, population // 4)
        current = 0
        while current < population:
            await asyncio.sleep(delay_seconds)
            current = min(population, current + step)
            await self.update_startup_check(
                run_id,
                "seed-handshake",
                StartupCheckUpdateRequest(
                    state="complete" if current == population else "running",
                    current=current,
                    detail=(
                        "seed manifest confirmed"
                        if current == population
                        else "confirming issued seed order"
                    ),
                ),
            )
        await self.update_startup_check(
            run_id,
            "eval-smoke",
            StartupCheckUpdateRequest(
                state="running",
                current=0,
                detail=f"smoking {run.recipe_label} evaluator",
            ),
        )
        await asyncio.sleep(delay_seconds)
        return await self.update_startup_check(
            run_id,
            "eval-smoke",
            StartupCheckUpdateRequest(state="complete", current=1, detail="evaluator smoke passed"),
        )

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
            defaults={
                "slice": "tail-4 + final RMSNorm",
                "paired_seeds": True,
                "artifact_bytes": 276_200_000,
            },
        ),
        RunRecipe(
            id="qwen05-gsm8k-fast",
            label="Qwen 0.5B GSM8K Fast",
            description="Cached local testnet recipe for fast multi-worker smoke runs.",
            model="mlx-community/Qwen2.5-0.5B-Instruct-4bit",
            evaluator="lighteval:gsm8k_fast",
            defaults={
                "slice": "tail-4 + final RMSNorm",
                "paired_seeds": True,
                "artifact_bytes": 276_200_000,
            },
        ),
        RunRecipe(
            id="qwen15-gsm8k",
            label="Qwen 1.5B GSM8K",
            description="Blessed lane recipe for the larger proposer/validator setup.",
            model="mlx-community/Qwen2.5-1.5B-Instruct-4bit",
            evaluator="lighteval:gsm8k",
            defaults={
                "slice": "tail-4 + final RMSNorm",
                "paired_seeds": True,
                "artifact_bytes": 869_000_000,
            },
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


def _make_seed_records(*, population: int, now_ms: int) -> list[SeedRecord]:
    return [
        SeedRecord(
            seed_id=f"seed-{index:06d}",
            sigma_id=f"sigma-{(index % 3) + 1}",
            issued_at_ms=now_ms + index,
            state="pending",
            rollouts=[],
        )
        for index in range(max(0, population))
    ]


def _normalize_run_settings(*, settings: dict[str, JsonScalar], profile: RunProfile) -> dict[str, JsonScalar]:
    normalized = dict(settings)
    max_workers = _positive_int_setting(normalized.get("max_workers"), profile.max_workers)
    default_quorum = min(2, max_workers)
    min_start_members = _positive_int_setting(
        normalized.get("min_start_members"),
        default_quorum,
    )
    normalized["max_workers"] = max_workers
    normalized["min_start_members"] = max(1, min(min_start_members, max_workers))
    normalized["population"] = _positive_int_setting(normalized.get("population"), profile.population)
    return normalized


def _positive_int_setting(value: JsonScalar | None, default: int) -> int:
    if isinstance(value, bool) or value is None:
        return max(1, default)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return max(1, default)


def _make_startup_checks(*, recipe: RunRecipe, population: int, now_ms: int) -> list[StartupCheck]:
    return [
        StartupCheck(
            id="base-weights",
            label="Downloading base weights",
            state="pending",
            current=0,
            total=100,
            detail=f"model {recipe.model}",
            updated_at_ms=now_ms,
        ),
        StartupCheck(
            id="seed-handshake",
            label="Connecting and confirming seeds",
            state="pending",
            current=0,
            total=max(1, population),
            detail="waiting to confirm deterministic seed manifest",
            updated_at_ms=now_ms,
        ),
        StartupCheck(
            id="eval-smoke",
            label="Confirming eval pipeline smoke",
            state="pending",
            current=0,
            total=1,
            detail=f"evaluator {recipe.evaluator}",
            updated_at_ms=now_ms,
        ),
    ]


def _run_status_from_startup(
    checks: list[StartupCheck],
    current_status: str,
) -> Literal["preparing", "running", "paused", "complete", "error"]:
    if current_status in {"paused", "complete", "error"} and not any(
        check.state == "failed" for check in checks
    ):
        return current_status  # Preserve explicit operator or terminal state.
    if any(check.state == "failed" for check in checks):
        return "error"
    if checks and all(check.state == "complete" for check in checks):
        return "running"
    return "preparing"


def _seed_state(rollouts: list[RolloutRecord]) -> Literal["pending", "leased", "completed"]:
    if not rollouts:
        return "pending"
    if all(row.status in {"completed", "failed"} for row in rollouts):
        return "completed"
    return "leased"
