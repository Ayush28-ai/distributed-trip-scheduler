-- Cloudstream distributed trip job scheduler: full schema.
-- Mounted into /docker-entrypoint-initdb.d, so it runs once on an empty volume.
-- After changing it: docker compose down -v && docker compose up --build

CREATE TABLE IF NOT EXISTS tenants (
    id               TEXT PRIMARY KEY,
    timezone         TEXT    NOT NULL DEFAULT 'UTC',
    paused           BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrency  INT     NOT NULL DEFAULT 50,      -- per-tenant cap (fairness)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       TEXT    NOT NULL REFERENCES tenants(id),
    name            TEXT    NOT NULL,
    job_type        TEXT    NOT NULL,                  -- pickup_alert | roster | billing | no_show | noop | flaky
    cron            TEXT,                              -- recurring job (NULL = one-off)
    run_at          TIMESTAMPTZ,                       -- one-off job
    timezone        TEXT    NOT NULL DEFAULT 'UTC',    -- cron is evaluated in this zone
    priority        INT     NOT NULL DEFAULT 5,        -- higher runs first
    max_runtime_s   INT     NOT NULL DEFAULT 30,
    max_attempts    INT     NOT NULL DEFAULT 5,
    misfire_policy  TEXT    NOT NULL DEFAULT 'FIRE_NOW' CHECK (misfire_policy IN ('FIRE_NOW','SKIP','COALESCE')),
    payload         JSONB   NOT NULL DEFAULT '{}'::jsonb,
    next_fire_at    TIMESTAMPTZ,                       -- cron cursor: first fire time not yet materialized
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((cron IS NULL) <> (run_at IS NULL))
);
CREATE INDEX IF NOT EXISTS jobs_cron_due ON jobs (next_fire_at) WHERE cron IS NOT NULL AND active;

-- Globally increasing fencing tokens: every claim takes a new, larger number.
CREATE SEQUENCE IF NOT EXISTS fencing_seq;

-- One row per firing. This is the unit that is claimed, leased, fenced and retried.
CREATE TABLE IF NOT EXISTS job_runs (
    id               BIGSERIAL PRIMARY KEY,
    job_id           BIGINT  NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    tenant_id        TEXT    NOT NULL,
    partition        INT     NOT NULL,                 -- job_id % 32
    scheduled_at     TIMESTAMPTZ NOT NULL,             -- intended fire time (UTC instant)
    priority         INT     NOT NULL DEFAULT 5,
    misfire_policy   TEXT    NOT NULL DEFAULT 'FIRE_NOW',
    status           TEXT    NOT NULL DEFAULT 'SCHEDULED'
                     CHECK (status IN ('SCHEDULED','RUNNING','RETRY_WAIT','SUCCEEDED','DEAD','SKIPPED')),
    attempt          INT     NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    owner_node       TEXT,
    lease_until      TIMESTAMPTZ,
    fencing_token    BIGINT  NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    fire_lag_ms      DOUBLE PRECISION,                 -- first claim time - scheduled_at (DB clock)
    last_error       TEXT,
    UNIQUE (job_id, scheduled_at)                      -- a firing is materialized at most once
);
CREATE INDEX IF NOT EXISTS runs_due      ON job_runs (next_attempt_at) WHERE status IN ('SCHEDULED','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS runs_due_ten  ON job_runs (tenant_id, next_attempt_at) WHERE status IN ('SCHEDULED','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS runs_running  ON job_runs (partition, lease_until) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS runs_tenant   ON job_runs (tenant_id, status);
CREATE INDEX IF NOT EXISTS runs_started  ON job_runs (started_at);

-- Partition leases: 32 small lock rows instead of one hot lock.
CREATE TABLE IF NOT EXISTS partitions (
    id           INT PRIMARY KEY,
    owner_node   TEXT,
    lease_until  TIMESTAMPTZ,
    epoch        BIGINT NOT NULL DEFAULT 0              -- increments on every ownership change
);
INSERT INTO partitions (id) SELECT g FROM generate_series(0, 31) g ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS nodes (
    node_id         TEXT PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Exactly-once effect ledger for local effects (unique idempotency key).
CREATE TABLE IF NOT EXISTS effects (
    idempotency_key  TEXT PRIMARY KEY,                 -- tenant:job:scheduled_at
    tenant_id        TEXT   NOT NULL,
    run_id           BIGINT NOT NULL,
    fencing_token    BIGINT NOT NULL,
    job_type         TEXT   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id          BIGSERIAL PRIMARY KEY,
    run_id      BIGINT NOT NULL UNIQUE REFERENCES job_runs(id) ON DELETE CASCADE,
    job_id      BIGINT NOT NULL,
    tenant_id   TEXT   NOT NULL,
    attempts    INT    NOT NULL,
    last_error  TEXT,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);

-- Owned by the Spring Boot notification-service (Python never writes here).
CREATE TABLE IF NOT EXISTS notifications (
    id               BIGSERIAL PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    tenant_id        TEXT NOT NULL,
    trip_id          TEXT,
    message          TEXT,
    fencing_token    BIGINT,
    node_id          TEXT,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notifications_tenant ON notifications (tenant_id);
