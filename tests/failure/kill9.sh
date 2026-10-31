#!/usr/bin/env bash
# kill -9 one node while 1,200 pickup alerts are in flight.
# Pass: all 1,200 runs SUCCEEDED and the Spring service holds exactly 1,200 distinct notifications.
# Run with the stack up:  ./tests/failure/kill9.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

NODE="${1:-node1}"
TENANT=kill9
N=1200
PSQL=(docker compose exec -T postgres psql -U "${POSTGRES_USER:-scheduler}" -d "${POSTGRES_DB:-scheduler}" -v ON_ERROR_STOP=1 -tA)
q() { "${PSQL[@]}" -c "$1"; }

echo "== 1. seed $N pickup alerts, due in 5s, each takes 3s (tenant $TENANT)"
"${PSQL[@]}" >/dev/null <<SQL
DELETE FROM notifications WHERE tenant_id = '$TENANT';
DELETE FROM effects WHERE tenant_id = '$TENANT';
DELETE FROM jobs WHERE tenant_id = '$TENANT';
INSERT INTO tenants (id, timezone, max_concurrency) VALUES ('$TENANT', 'Asia/Kolkata', 600)
  ON CONFLICT (id) DO UPDATE SET paused = FALSE, max_concurrency = 600;
WITH j AS (
  INSERT INTO jobs (tenant_id, name, job_type, run_at, payload, max_runtime_s)
  SELECT '$TENANT', 'pickup-' || g, 'pickup_alert', date_trunc('milliseconds', now() + interval '5 seconds'),
         jsonb_build_object('trip_id', 'TRIP-' || g, 'simulate_ms', 3000), 30
    FROM generate_series(1, $N) g
  RETURNING id, tenant_id, run_at, priority, misfire_policy)
INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at, priority, misfire_policy)
SELECT id, tenant_id, id % 32, run_at, run_at, priority, misfire_policy FROM j;
SQL

sleep 6.5
INFLIGHT=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'RUNNING' AND owner_node = '$NODE'")
echo "== 2. kill -9 $NODE with $INFLIGHT alerts in flight on it"
docker compose kill -s SIGKILL "$NODE" >/dev/null
KILLED_AT=$(date +%s)

echo "== 3. wait for the other nodes to take over"
for _ in $(seq 1 60); do
  DONE=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'SUCCEEDED'")
  echo "   succeeded $DONE / $N"
  [ "$DONE" -ge "$N" ] && break
  sleep 2
done
RECOVERY=$(( $(date +%s) - KILLED_AT ))

TOTAL=$(q "SELECT count(*) FROM notifications WHERE tenant_id = '$TENANT'")
DISTINCT=$(q "SELECT count(DISTINCT idempotency_key) FROM notifications WHERE tenant_id = '$TENANT'")
TAKEN=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND attempt > 1")
DEAD=$(q "SELECT count(*) FROM job_runs WHERE tenant_id = '$TENANT' AND status = 'DEAD'")

echo
echo "== result"
echo "   runs succeeded:           $DONE / $N"
echo "   notifications sent:       $TOTAL (distinct keys $DISTINCT)"
echo "   runs taken over (attempt>1): $TAKEN"
echo "   dead:                     $DEAD"
echo "   all done ${RECOVERY}s after the kill"

echo "== restarting $NODE"
docker compose up -d "$NODE" >/dev/null

if [ "$DONE" -eq "$N" ] && [ "$TOTAL" -eq "$N" ] && [ "$DISTINCT" -eq "$N" ]; then
  echo "PASS: nothing lost, nothing sent twice"
else
  echo "FAIL"; exit 1
fi
