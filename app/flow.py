"""Deterministic integral max-flow used by the diversion trial.

The diversion problem is a bipartite flow problem:

* source -> domain, capacity = requests the offline gateway serves for it;
* domain -> gateway, capacity = the domain's requests, only for gateways the
  operator allows for that domain;
* gateway -> sink, capacity = the gateway's remaining period capacity.

Because all capacities are integers, an integral max-flow assigns whole requests.
The graph is built in a fixed order (domains sorted, gateways in the job's shard
order) and the DFS uses adjacency order, so the same input always returns the
same allocation. The *value* of the flow is unique; one of several optimal
splits is returned deterministically.

This module has no dependency on the HTTP layer or on job storage.
"""

from __future__ import annotations

from dataclasses import dataclass


class _Edge:
    __slots__ = ("to", "rev", "cap")

    def __init__(self, to: int, rev: int, cap: int) -> None:
        self.to = to
        self.rev = rev
        self.cap = cap


@dataclass(frozen=True)
class FlowResult:
    total: int
    # domain -> gateway -> whole requests assigned
    assignments: dict[str, dict[str, int]]


class _Dinic:
    def __init__(self, node_count: int) -> None:
        self.graph: list[list[_Edge]] = [[] for _ in range(node_count)]

    def add_edge(self, source: int, target: int, capacity: int) -> _Edge:
        forward = _Edge(target, len(self.graph[target]), capacity)
        backward = _Edge(source, len(self.graph[source]), 0)
        self.graph[source].append(forward)
        self.graph[target].append(backward)
        return forward

    def _bfs(self, source: int, sink: int) -> list[int]:
        level = [-1] * len(self.graph)
        level[source] = 0
        queue = [source]
        for node in queue:
            for edge in self.graph[node]:
                if edge.cap > 0 and level[edge.to] < 0:
                    level[edge.to] = level[node] + 1
                    queue.append(edge.to)
        return level

    def _dfs(self, node: int, sink: int, limit: int, level: list[int], it: list[int]) -> int:
        if node == sink:
            return limit
        while it[node] < len(self.graph[node]):
            edge = self.graph[node][it[node]]
            if edge.cap > 0 and level[edge.to] == level[node] + 1:
                pushed = self._dfs(edge.to, sink, min(limit, edge.cap), level, it)
                if pushed:
                    edge.cap -= pushed
                    self.graph[edge.to][edge.rev].cap += pushed
                    return pushed
            it[node] += 1
        return 0

    def max_flow(self, source: int, sink: int, upper_bound: int) -> int:
        flow = 0
        infinity = upper_bound if upper_bound > 0 else 1
        while flow < upper_bound:
            level = self._bfs(source, sink)
            if level[sink] < 0:
                break
            it = [0] * len(self.graph)
            while flow < upper_bound:
                pushed = self._dfs(source, sink, infinity, level, it)
                if not pushed:
                    break
                flow += pushed
        return flow


def max_split_flow(
    request_by_domain: dict[str, int],
    allowed_targets: dict[str, list[str]],
    gateway_capacity: dict[str, int],
) -> FlowResult:
    """Assign as many requests as possible from domains to gateways.

    ``request_by_domain`` only contains domains with requests to move.
    ``allowed_targets`` maps each such domain to gateways allowed to receive it.
    ``gateway_capacity`` maps every remaining gateway to its remaining capacity.
    A gateway with zero capacity simply receives nothing.
    """

    domains = sorted(request_by_domain)
    gateways = list(gateway_capacity)
    if not domains or not gateways:
        return FlowResult(total=0, assignments={})

    source = 0
    domain_base = 1
    gateway_base = domain_base + len(domains)
    sink = gateway_base + len(gateways)
    dinic = _Dinic(sink + 1)
    gateway_index = {gateway: offset for offset, gateway in enumerate(gateways)}

    for offset, domain in enumerate(domains):
        node = domain_base + offset
        dinic.add_edge(source, node, request_by_domain[domain])

    for gateway in gateways:
        node = gateway_base + gateway_index[gateway]
        dinic.add_edge(node, sink, max(0, gateway_capacity[gateway]))

    domain_target_edges: dict[str, dict[str, _Edge]] = {domain: {} for domain in domains}
    allowed_set = {domain: set(allowed_targets.get(domain, [])) for domain in domains}
    for offset, domain in enumerate(domains):
        node = domain_base + offset
        for gateway in gateways:
            if gateway not in allowed_set[domain]:
                continue
            edge = dinic.add_edge(
                node, gateway_base + gateway_index[gateway], request_by_domain[domain]
            )
            domain_target_edges[domain][gateway] = edge

    upper_bound = sum(request_by_domain.values())
    total = dinic.max_flow(source, sink, upper_bound)

    assignments: dict[str, dict[str, int]] = {}
    for domain in domains:
        per_gateway: dict[str, int] = {}
        for gateway, edge in domain_target_edges[domain].items():
            used = request_by_domain[domain] - edge.cap
            if used > 0:
                per_gateway[gateway] = used
        assignments[domain] = per_gateway
    return FlowResult(total=total, assignments=assignments)
