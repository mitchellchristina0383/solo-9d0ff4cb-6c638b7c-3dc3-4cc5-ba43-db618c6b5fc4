"""Unit tests for the integral max-flow allocator behind the diversion trial."""

from __future__ import annotations

import itertools
import random

from app.flow import max_split_flow


def test_augmenting_path_reroutes_a_gateway() -> None:
    # a can use either gateway, b only gw-1. A greedy a->gw-1 leaves b stuck,
    # total 1; the optimum reroutes a to gw-2 and gets 2.
    result = max_split_flow(
        {"a.example": 1, "b.example": 1},
        {"a.example": ["gw-1", "gw-2"], "b.example": ["gw-1"]},
        {"gw-1": 1, "gw-2": 1},
    )
    assert result.total == 2
    assert result.assignments["a.example"] == {"gw-2": 1}
    assert result.assignments["b.example"] == {"gw-1": 1}


def test_zero_capacity_gateway_is_skipped() -> None:
    result = max_split_flow(
        {"a.example": 3},
        {"a.example": ["gw-1", "gw-2"]},
        {"gw-1": 0, "gw-2": 2},
    )
    assert result.total == 2
    assert result.assignments["a.example"] == {"gw-2": 2}


def test_no_domains_or_no_gateways() -> None:
    assert max_split_flow({}, {}, {}).total == 0
    assert max_split_flow({"a.example": 1}, {"a.example": []}, {"gw-1": 5}).total == 0


def _brute_force(
    request_by_domain: dict[str, int],
    allowed_targets: dict[str, list[str]],
    capacity: dict[str, int],
) -> int:
    """Exhaustive optimum for tiny instances, used as an oracle."""

    domains = sorted(request_by_domain)
    per_domain_options = []
    for domain in domains:
        amount = request_by_domain[domain]
        targets = allowed_targets[domain]
        options = []
        for counts in itertools.product(range(amount + 1), repeat=len(targets)):
            if sum(counts) <= amount:  # the rest stays unmigrated
                options.append(dict(zip(targets, counts)))
        per_domain_options.append(options)

    best = 0
    for combination in itertools.product(*per_domain_options):
        used: dict[str, int] = {gateway: 0 for gateway in capacity}
        total = 0
        for allocation in combination:
            for gateway, amount in allocation.items():
                used[gateway] += amount
            total += sum(allocation.values())
        if all(used[gateway] <= capacity[gateway] for gateway in capacity):
            best = max(best, total)
    return best


def test_matches_brute_force_on_random_small_instances() -> None:
    rng = random.Random(20261124)
    gateways = ["gw-1", "gw-2", "gw-3"]
    for _ in range(150):
        domains = [f"d{i}.example" for i in range(rng.randint(0, 3))]
        request_by_domain = {d: rng.randint(0, 3) for d in domains}
        allowed = {
            d: [g for g in gateways if rng.random() < 0.7] for d in domains
        }
        capacity = {g: rng.randint(0, 4) for g in gateways}
        expected = _brute_force(request_by_domain, allowed, capacity)
        result = max_split_flow(request_by_domain, allowed, capacity)
        assert result.total == expected, (request_by_domain, allowed, capacity)
        # Assignments must be feasible and consistent with the reported total.
        per_gateway: dict[str, int] = {g: 0 for g in gateways}
        assignment_total = 0
        for domain in domains:
            assigned = result.assignments[domain]
            assert set(assigned) <= set(allowed[domain])
            assert sum(assigned.values()) <= request_by_domain[domain]
            assignment_total += sum(assigned.values())
            for gateway, amount in assigned.items():
                per_gateway[gateway] += amount
        assert assignment_total == result.total
        for gateway in gateways:
            assert per_gateway[gateway] <= capacity[gateway]


def test_assignment_is_stable_for_the_same_input() -> None:
    request_by_domain = {"a.example": 5, "b.example": 5, "c.example": 5}
    allowed = {domain: ["gw-1", "gw-2"] for domain in request_by_domain}
    capacity = {"gw-1": 7, "gw-2": 7}
    results = {max_split_flow(request_by_domain, allowed, capacity).__repr__() for _ in range(5)}
    assert len(results) == 1
