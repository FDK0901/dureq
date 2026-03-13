-- dureq: PostgreSQL init script for transactional completion examples.
-- This file is mounted into the postgres container via docker-compose.

-- Completion ledger used by pkg/integration/postgres.
CREATE TABLE IF NOT EXISTS dureq_completions (
    run_id       TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    step         TEXT NOT NULL DEFAULT '',
    data         JSONB,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, step)
);

-- Example business table used by examples/pg_completion.
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    amount   NUMERIC NOT NULL,
    status   TEXT NOT NULL DEFAULT 'pending'
);
