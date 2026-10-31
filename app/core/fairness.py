"""Fair sharing of a node's free capacity across tenants. Pure: no DB, no web.

demand[t] = min(due runs of t, tenant cap - currently running of t).
Water-filling: every tenant with demand gets an equal share; what a small tenant
does not need is handed to the others. A tenant with 50,000 due alerts therefore
cannot push out a tenant with 20.
"""


def fair_allocate(capacity: int, demand: dict[str, int]) -> dict[str, int]:
    alloc = {t: 0 for t in demand}
    remaining = max(0, capacity)
    active = sorted(t for t, d in demand.items() if d > 0)
    while remaining > 0 and active:
        share = max(1, remaining // len(active))
        still_active = []
        for t in active:
            if remaining == 0:
                still_active.append(t)
                continue
            give = min(share, demand[t] - alloc[t], remaining)
            alloc[t] += give
            remaining -= give
            if alloc[t] < demand[t]:
                still_active.append(t)
        active = still_active
    return {t: a for t, a in alloc.items() if a > 0}


def tenant_demand(due: dict[str, int], running: dict[str, int], caps: dict[str, int]) -> dict[str, int]:
    return {t: max(0, min(n, caps.get(t, 50) - running.get(t, 0))) for t, n in due.items()}
