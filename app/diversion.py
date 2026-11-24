"""Diversion trial calculation.

A trial is a snapshot of one **complete** published aggregation. From that single
snapshot it answers: if ``offline_shard_id`` is taken out of service, how many of
its requests can the remaining gateways absorb within their period limits?

Rules implemented here (see README for the API contract):

* every remaining gateway's limit is reduced by **all** requests it already
  serves in the snapshot (summed over every published domain); a gateway whose
  existing load exceeds its limit has zero remaining capacity;
* remaining capacity is shared by all affected domains;
* each affected domain may only use the gateways listed for it, and may split
  its requests across several of them;
* the maximum migratable request count is an integral max-flow value;
* if the published summary is incomplete, only the missing items are reported
  and no plan is computed.
"""

from __future__ import annotations

import hashlib
import json

from .flow import max_split_flow
from .models import (
    DiversionAssignment,
    DiversionDomainPlan,
    DiversionPlanStatus,
    DiversionTrial,
    DiversionTrialRequest,
    GatewayCapacity,
    JobState,
    MissingItem,
    MissingKind,
    PartitionStatus,
)
from .normalize import normalize_domain


class DiversionInputError(ValueError):
    """The request contradicts the job or its published snapshot."""


# --------------------------------------------------------------- snapshot
def published_request_counts(job: JobState) -> dict[str, dict[str, int]]:
    """Per-domain, per-shard request counts from *published* partitions only."""

    counts: dict[str, dict[str, int]] = {}
    for domain in sorted(job.partitions):
        partition = job.partitions[domain]
        if partition.status != PartitionStatus.PUBLISHED:
            continue
        counts[domain] = {
            shard_id: contribution.total_requests
            for shard_id, contribution in partition.contributions.items()
        }
    return counts


def results_fingerprint(counts: dict[str, dict[str, int]]) -> str:
    canonical = json.dumps(counts, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def summary_missing(job: JobState) -> list[MissingItem]:
    """Everything that prevents treating the job's publication as complete.

    An empty list means a complete published summary: every shard has a trusted
    snapshot (a run that is merely in progress is fine — the published
    partitions still reflect the last verified scan), no parts are stale, and
    every known partition is published.
    """

    missing: list[MissingItem] = []

    # ``PROCESSING`` only means a run is in flight; completed shards keep their
    # verified snapshots and partitions, so it does not by itself make the
    # currently *published* summary incomplete. A failed re-read after an earlier
    # good read is likewise fine as long as the parts were not invalidated: the
    # merge can only have published verified contributions.
    unscanned = [sid for sid in job.shard_ids if job.shards[sid].successful_reads == 0]
    if unscanned:
        missing.append(
            MissingItem(
                kind=MissingKind.UNSCANNED_SHARD,
                shard_ids=unscanned,
                detail="source shards have no successfully verified snapshot",
            )
        )

    stale = [sid for sid in job.shard_ids if job.shards[sid].stale_parts]
    if stale:
        missing.append(
            MissingItem(
                kind=MissingKind.STALE_SHARD,
                shard_ids=stale,
                detail="temporary parts are stale after a failed rescan",
            )
        )

    for domain in sorted(job.partitions):
        partition = job.partitions[domain]
        if partition.status == PartitionStatus.BLOCKED:
            missing.append(
                MissingItem(
                    kind=MissingKind.BLOCKED_DOMAIN,
                    domain=domain,
                    shard_ids=list(partition.blocked_shards),
                    detail=partition.error or "partition is withheld",
                )
            )
        elif partition.status == PartitionStatus.PENDING:
            missing.append(MissingItem(kind=MissingKind.PENDING_DOMAIN, domain=domain))

    if not job.partitions:
        missing.append(
            MissingItem(
                kind=MissingKind.NO_PUBLISHED_RESULTS,
                detail="job has no published partitions yet; run the job first",
            )
        )
    return missing


def describe_missing(missing: list[MissingItem]) -> str:
    parts: list[str] = []
    for item in missing:
        if item.kind in (MissingKind.UNSCANNED_SHARD, MissingKind.STALE_SHARD):
            label = "unscanned" if item.kind == MissingKind.UNSCANNED_SHARD else "stale"
            parts.append(f"{label} shards: " + ", ".join(item.shard_ids))
        elif item.kind == MissingKind.BLOCKED_DOMAIN:
            suffix = f" ({', '.join(item.shard_ids)})" if item.shard_ids else ""
            parts.append(f"blocked domain {item.domain}{suffix}")
        elif item.kind == MissingKind.PENDING_DOMAIN:
            parts.append(f"pending domain {item.domain}")
        elif item.detail:
            parts.append(item.detail)
    return "; ".join(parts)


# ------------------------------------------------------------ validation
def _normalize_allowed_targets(
    job: JobState,
    offline_shard_id: str,
    raw: dict[str, list[str]],
    remaining: set[str],
) -> dict[str, list[str]]:
    allowed: dict[str, list[str]] = {}
    for raw_domain, targets in raw.items():
        domain = normalize_domain(raw_domain)
        if domain is None:
            raise DiversionInputError(f"invalid domain in domain_allowed_targets: {raw_domain!r}")
        if domain in allowed:
            raise DiversionInputError(
                f"domain {raw_domain!r} appears more than once after normalization"
            )
        for target in targets:
            if target == offline_shard_id:
                raise DiversionInputError(
                    f"domain {domain} lists the offline gateway {offline_shard_id} as a target"
                )
            if target not in remaining:
                raise DiversionInputError(
                    f"domain {domain} lists {target!r}, which is not a remaining gateway of this job"
                )
        # Keep the job's shard order for deterministic output.
        allowed[domain] = [sid for sid in job.shard_ids if sid in set(targets)]
    return allowed


# -------------------------------------------------------------- planning
def build_trial(job: JobState, trial_id: str, request: DiversionTrialRequest) -> DiversionTrial:
    offline = request.offline_shard_id
    if offline not in job.shards:
        raise DiversionInputError(f"offline gateway {offline!r} is not a shard of this job")

    remaining = [sid for sid in job.shard_ids if sid != offline]
    remaining_set = set(remaining)
    if len(remaining) != len(job.shard_ids) - 1:  # pragma: no cover - guarded above
        raise DiversionInputError("offline gateway matches more than one shard")

    limits = request.gateway_request_limits
    missing_limits = [sid for sid in remaining if sid not in limits]
    extra_limits = sorted(set(limits) - remaining_set)
    if missing_limits or extra_limits:
        problems = []
        if missing_limits:
            problems.append("missing limits for: " + ", ".join(missing_limits))
        if extra_limits:
            problems.append("limits for gateways that are not remaining: " + ", ".join(extra_limits))
        raise DiversionInputError(
            "gateway_request_limits must name exactly the job's gateways except the offline one ("
            + "; ".join(problems)
            + ")"
        )

    allowed = _normalize_allowed_targets(job, offline, request.domain_allowed_targets, remaining_set)

    trial = DiversionTrial(
        trial_id=trial_id,
        job_id=job.job_id,
        offline_shard_id=offline,
        job_status_at_creation=job.status,
        gateway_request_limits={sid: limits[sid] for sid in remaining},
        domain_allowed_targets=allowed,
        summary_complete=False,
        plan_status=DiversionPlanStatus.INCOMPLETE,
    )

    missing = summary_missing(job)
    if missing:
        # Incomplete summary: report only what is missing, compute nothing.
        trial.missing = missing
        trial.fully_divertible = None
        trial.warnings = ["summary is incomplete; only the missing items are listed"]
        return trial

    counts = published_request_counts(job)
    trial.summary_complete = True
    trial.results_fingerprint = results_fingerprint(counts)

    affected = {
        domain: domain_counts.get(offline, 0)
        for domain, domain_counts in counts.items()
        if domain_counts.get(offline, 0) > 0
    }
    if not affected:
        trial.warnings.append(
            f"offline gateway {offline} has no published requests; nothing needs to be moved"
        )
    expected_domains = set(affected)
    supplied_domains = set(allowed)
    if supplied_domains != expected_domains:
        not_supplied = sorted(expected_domains - supplied_domains)
        not_affected = sorted(supplied_domains - expected_domains)
        problems = []
        if not_supplied:
            problems.append("missing allowed targets for: " + ", ".join(not_supplied))
        if not_affected:
            problems.append(
                "domains not served by the offline gateway: " + ", ".join(not_affected)
            )
        raise DiversionInputError(
            "domain_allowed_targets must name exactly the domains the offline gateway serves ("
            + "; ".join(problems)
            + ")"
        )

    existing = {
        sid: sum(domain_counts.get(sid, 0) for domain_counts in counts.values())
        for sid in remaining
    }
    remaining_capacity: dict[str, int] = {}
    capacity_rows: list[GatewayCapacity] = []
    for sid in remaining:
        limit = limits[sid]
        over = existing[sid] > limit
        free = max(0, limit - existing[sid])
        remaining_capacity[sid] = free
        capacity_rows.append(
            GatewayCapacity(
                shard_id=sid,
                request_limit=limit,
                existing_requests=existing[sid],
                remaining_capacity=free,
                over_limit=over,
            )
        )
        if over:
            trial.warnings.append(
                f"gateway {sid} already serves {existing[sid]} requests, above its limit of "
                f"{limit}; it is treated as having no remaining capacity"
            )

    flow = max_split_flow(affected, allowed, remaining_capacity)

    assigned_per_gateway = {sid: 0 for sid in remaining}
    plans: list[DiversionDomainPlan] = []
    for domain in sorted(affected):
        per_gateway = flow.assignments.get(domain, {})
        assignments = [
            DiversionAssignment(shard_id=sid, requests=per_gateway[sid])
            for sid in remaining
            if per_gateway.get(sid, 0) > 0
        ]
        migrated = sum(item.requests for item in assignments)
        plans.append(
            DiversionDomainPlan(
                domain=domain,
                requests_to_move=affected[domain],
                migrated_requests=migrated,
                unmigrated_requests=affected[domain] - migrated,
                assignments=assignments,
            )
        )
        for item in assignments:
            assigned_per_gateway[item.shard_id] += item.requests
        if not allowed.get(domain):
            trial.warnings.append(
                f"domain {domain} has no allowed target gateway; "
                f"{affected[domain]} request(s) cannot be moved"
            )

    for row in capacity_rows:
        row.assigned_requests = assigned_per_gateway[row.shard_id]
        row.final_requests = row.existing_requests + row.assigned_requests

    total_to_move = sum(affected.values())
    total_migrated = sum(plan.migrated_requests for plan in plans)
    trial.gateway_capacity = capacity_rows
    trial.domains = plans
    trial.total_requests_to_move = total_to_move
    trial.total_migrated_requests = total_migrated
    trial.total_unmigrated_requests = total_to_move - total_migrated
    if total_migrated == total_to_move:
        trial.plan_status = DiversionPlanStatus.FULL
        trial.fully_divertible = True
    else:
        trial.plan_status = DiversionPlanStatus.PARTIAL
        trial.fully_divertible = False
    return trial


def confirmable(trial: DiversionTrial, job: JobState) -> tuple[bool, str | None, str]:
    """Return ``(allowed, error_code, reason)`` for confirming ``trial`` now.

    A confirmation is only accepted when the trial was complete and fully
    divertible, the summary is still complete, and the per-domain per-shard
    request counts are identical to the trial snapshot. ``error_code`` tells the
    HTTP layer which class of failure this is:

    * ``trial_incomplete`` / ``trial_not_fully_divertible`` — the trial itself
      can never be confirmed (400);
    * ``summary_incomplete_now`` / ``results_changed`` — the trial was valid but
      the job moved on, so it must be recalculated (409).
    """

    if trial.confirmed_at is not None:
        return True, None, ""
    if not trial.summary_complete or trial.plan_status == DiversionPlanStatus.INCOMPLETE:
        return (
            False,
            "trial_incomplete",
            "this trial was created from an incomplete summary; "
            "recalculate the diversion trial after the summary is complete",
        )
    if not trial.fully_divertible:
        return (
            False,
            "trial_not_fully_divertible",
            f"this trial cannot move all requests ({trial.total_unmigrated_requests} would "
            "remain); only a fully divertible trial can be confirmed. Adjust limits or allowed "
            "targets and recalculate",
        )
    missing = summary_missing(job)
    if missing:
        return (
            False,
            "summary_incomplete_now",
            "the published summary is incomplete now ("
            + describe_missing(missing)
            + "); recalculate the diversion trial",
        )
    current = results_fingerprint(published_request_counts(job))
    if current != trial.results_fingerprint:
        return (
            False,
            "results_changed",
            "per-domain per-shard request counts changed after the trial was calculated; "
            "recalculate the diversion trial",
        )
    return True, None, ""
