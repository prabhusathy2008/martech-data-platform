CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_events (
    event_id TEXT PRIMARY KEY,
    event_ts TEXT,
    user_id TEXT,
    user_login TEXT,
    repo_source TEXT,
    event_type TEXT,
    payload JSONB,
    ingested_file TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
