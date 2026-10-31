"""Heartbeats + partition leases.

Every tick: heartbeat, read live nodes, compute which partitions this node should own
(rendezvous hashing), take/renew those leases, release the rest. When a node dies its
heartbeat stops, the others recompute, and its partitions move once its leases expire.

Partition ownership spreads the work and avoids one hot lock row. It is NOT what makes
execution safe: that is the row-level claim + fencing token (see dispatcher/executor).
"""
import asyncio
import logging
import time

from app import repository as repo
from app.core.partitioning import owner_for
from app.log_config import log_event
from app.metrics import OWNED_PARTITIONS

log = logging.getLogger("membership")


class Membership:
    def __init__(self, pool, settings):
        self.pool = pool
        self.s = settings
        self._owned: list[int] = []
        self._valid_until = 0.0  # monotonic; keeps running even while the process is SIGSTOPped

    @property
    def owned(self) -> list[int]:
        """Owned partitions, or [] if our lease may have expired (e.g. we were frozen)."""
        return self._owned if time.monotonic() < self._valid_until else []

    async def tick(self) -> None:
        started = time.monotonic()
        await repo.heartbeat(self.pool, self.s.node_id)
        live = await repo.live_nodes(self.pool, self.s.node_dead_after_s)
        if self.s.node_id not in live:
            live.append(self.s.node_id)
        desired = [p for p in range(self.s.num_partitions) if owner_for(p, live) == self.s.node_id]
        await repo.release_partitions(self.pool, self.s.node_id, desired)
        owned = await repo.acquire_partitions(self.pool, self.s.node_id, desired,
                                              self.s.partition_lease_s)
        if owned != self._owned:
            log_event(log, "partitions_changed", node=self.s.node_id, live_nodes=sorted(live),
                      owned=owned, waiting_for=sorted(set(desired) - set(owned)))
        self._owned = owned
        # trust our view for a bit less than the lease, measured from before the DB calls
        self._valid_until = started + self.s.partition_lease_s * 0.8
        OWNED_PARTITIONS.set(len(owned))

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                log.exception("membership_tick_failed")
            await asyncio.sleep(self.s.heartbeat_s)
