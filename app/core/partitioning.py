"""Partitioning. Pure: no DB, no web.

- A job lives in partition job_id % N (round-robin, balanced, computable in SQL too).
- A partition is owned by the live node with the highest rendezvous score.
  When a node joins or leaves, only the partitions it gains or loses move.
"""
import hashlib


def partition_for(job_id: int, num_partitions: int = 32) -> int:
    return job_id % num_partitions


def _score(node: str, partition: int) -> int:
    return int(hashlib.sha1(f"{node}:{partition}".encode()).hexdigest()[:16], 16)


def owner_for(partition: int, nodes: list[str]) -> str | None:
    if not nodes:
        return None
    return max(sorted(nodes), key=lambda n: _score(n, partition))


def assign(num_partitions: int, nodes: list[str]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {n: [] for n in nodes}
    for p in range(num_partitions):
        owner = owner_for(p, nodes)
        if owner is not None:
            out[owner].append(p)
    return out
