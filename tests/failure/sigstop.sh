#!/usr/bin/env bash
# Freeze one node with SIGSTOP for 30s while it holds in-flight alerts, then SIGCONT.
# Pass: other nodes take over, every alert is sent exactly once, and the frozen node's
# late writes are rejected by the fencing token (stale_commit_rejected in its logs).
# Run with the stack up:  ./tests/failure/sigstop.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

NODE="${1:-node2}"
TENANT=sigstop
N=600
FREEZE_S=30
PSQL=(docker compose exec -T postgres psql -U "${POSTGRES_USER:-scheduler}" -d "${POSTGRES_DB:-scheduler}" -v ON_ERROR_STOP=1 -tA)
q() { "${PSQL[@]}" -c "$1"; }

echo "== 1. seed $N pickup alerts, due in 5s, each takes 4s (tenant $TENANT)"
"${PSQL[@]}" >/dev/null <<SQL
DELETE FROM notifications WHERE tenant_id = '$TENANT';
DELETE FROM effects WHERE tenant_id = '$TENANT';
DELETE FROM jobs WHERE tenant_id = '$TENANT';
INSERT INTO tenants (id, timezone, max_concurrency) VALUES ('$TENANT', 'UTC', 600)
  ON CONFLICT (id) DO UPDATE SET paused = FALSE, max_concurrency = 600;
WITH j AS (
  INSERT INTO jobs (tenant_id, name, job_type, run_at, payload, max_runtime_s)
  SELECT '$TENANT', 'pickup-' || g, 'pickup_alert', date_trunc('milliseconds', now() + interval '5 seconds'),
         jsonb_build_object('trip_id', 'TRIP-' || g, 'simulate_ms', 4000), 60
    FROM generate_series(1, $N) g
  RETURNING id, tenant_id, run_at, priority, misfire_policy)
INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at, priority, misfire_policy)
SELECT id, tenant_id, id % 32, run_at, run_at, priority, misfire_policy FROM j;
SQL

sleep 6.5
INFLIGHT=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'RUNNING' AND owner_node = '$NODE'")
SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "== 2. SIGSTOP $NODE for ${FREEZE_S}s with $INFLIGHT alerts in flight on it"
docker compose kill -s SIGSTOP "$NODE" >/dev/null
sleep "$FREEZE_S"
DONE_WHILE_FROZEN=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'SUCCEEDED'")
echo "   while frozen, the cluster finished $DONE_WHILE_FROZEN / $N"
echo "== 3. SIGCONT $NODE: it wakes up still believing it owns those runs"
docker compose kill -s SIGCONT "$NODE" >/dev/null
sleep 12

DONE=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'SUCCEEDED'")
TOTAL=$(q "SELECT count(*) FROM notifications WHERE tenant_id = '$TENANT'")
DISTINCT=$(q "SELECT count(DISTINCT idempotency_key) FROM notifications WHERE tenant_id = '$TENANT'")
REJECTED=$(docker compose logs --no-log-prefix --since "$SINCE" "$NODE" 2>/dev/null | grep -c '"stale_commit_rejected"' || true)
LOST=$(docker compose logs --no-log-prefix --since "$SINCE" "$NODE" 2>/dev/null | grep -c '"leases_lost"' || true)

echo
echo "== result"
echo "   runs succeeded:              $DONE / $N"
echo "   notifications sent:          $TOTAL (distinct keys $DISTINCT)"
echo "   stale writes rejected on $NODE: $REJECTED  (lease-lost warnings: $LOST)"

if [ "$DONE" -eq "$N" ] && [ "$TOTAL" -eq "$N" ] && [ "$DISTINCT" -eq "$N" ] && [ "$REJECTED" -gt 0 ]; then
  echo "PASS: frozen node's stale writes rejected, every alert sent exactly once"
else
  echo "FAIL"; exit 1
fi
