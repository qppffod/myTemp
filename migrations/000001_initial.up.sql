-- 000001_initial.up.sql

CREATE TABLE workflow_executions (
    workflow_id     TEXT NOT NULL,
    run_id          UUID NOT NULL DEFAULT gen_random_uuid(),
    workflow_type   TEXT NOT NULL,
    task_queue      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'Running',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, run_id)
);

CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    workflow_id     TEXT NOT NULL,
    run_id          UUID NOT NULL,
    event_id        BIGINT NOT NULL,
    event_type      TEXT NOT NULL,
    activity_name   TEXT NOT NULL DEFAULT '',
    activity_index  INT NOT NULL DEFAULT 0,
    data            BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id                  BIGSERIAL PRIMARY KEY,
    task_queue          TEXT NOT NULL,
    task_type           TEXT NOT NULL,           
    workflow_type       TEXT NOT NULL,
    workflow_id         TEXT NOT NULL,
    run_id              UUID NOT NULL,
    scheduled_event_id  BIGINT NOT NULL,
    input               BYTEA,
    activity_name       TEXT,
    activity_index      INT NOT NULL DEFAULT 0,
    attempt             INT NOT NULL DEFAULT 1,
    max_attempts        INT NOT NULL DEFAULT 3,
    visibility_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner         TEXT,
    lease_expires_at    TIMESTAMPTZ
);

CREATE INDEX idx_tasks_poll ON tasks (task_queue, visibility_time)
    WHERE lease_owner IS NULL;