from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import psycopg
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )

@dataclass(frozen=True)
class Settings:
    github_org: str
    raw_bucket: str
    raw_prefix: str
    operational_bucket: str
    checkpoint_key: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str

def load_settings() -> Settings:
    org = os.getenv("GITHUB_ORG", "adevinta")

    # Running inside cluster: use service DNS name.
    # Running locally from laptop: use NodePort exposed on localhost.
    default_minio_endpoint = (
        "http://minio:9000"
        if os.getenv("KUBERNETES_SERVICE_HOST")
        else "http://localhost:9000"
    )
    default_postgres_host = "postgres" if os.getenv("KUBERNETES_SERVICE_HOST") else "localhost"

    return Settings(
        github_org=org,
        raw_bucket=os.getenv("RAW_BUCKET", "dl-raw-events"),
        raw_prefix=os.getenv("RAW_PREFIX", f"source=github/org={org}"),
        operational_bucket=os.getenv("OPERATIONAL_BUCKET", "ops-pipelines"),
        checkpoint_key=os.getenv("CHECKPOINT_KEY", f"dwh-loader/events/github/{org}/checkpoint.json"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", default_minio_endpoint),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY"),
        postgres_host=os.getenv("POSTGRES_HOST", default_postgres_host),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "martech"),
        postgres_user=os.getenv("POSTGRES_USER"),
        postgres_password=os.getenv("POSTGRES_PASSWORD"),
    )

def build_s3_client(settings: Settings) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def load_checkpoint(s3: BaseClient, settings: Settings) -> set[str]:
    try:
        response = s3.get_object(Bucket=settings.operational_bucket, Key=settings.checkpoint_key)
        payload = json.loads(response["Body"].read())
        files = payload.get("processed_files", [])
        return set(files)
    except ClientError as exc:
        code = exc.response["Error"].get("Code")
        if code in ("NoSuchKey", "NoSuchBucket", "404"):
            return set()
        raise

def save_checkpoint(s3: BaseClient, settings: Settings, processed_files: set[str]) -> None:
    payload = {
        "processed_files": sorted(processed_files),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    s3.put_object(
        Bucket=settings.operational_bucket,
        Key=settings.checkpoint_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

def list_raw_objects(s3: BaseClient, settings: Settings) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=settings.raw_bucket, Prefix=settings.raw_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".ndjson"):
                keys.append(key)

    return sorted(keys)

def _ddl_sql() -> str:
    ddl_path = Path(__file__).resolve().parent / "sql" / "raw_raw_events.sql"
    return ddl_path.read_text(encoding="utf-8")

def ensure_raw_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(_ddl_sql())
    conn.commit()

def upsert_events(
    conn: psycopg.Connection,
    events: list[dict[str, Any]],
    source_key: str,
) -> int:
    if not events:
        return 0

    inserted = 0
    with conn.cursor() as cursor:
        for event in events:
            actor = event.get("actor") or {}
            repo = event.get("repo") or {}

            cursor.execute(
                """
                INSERT INTO raw.raw_events (
                    event_id,
                    event_ts,
                    user_id,
                    user_login,
                    repo_source,
                    event_type,
                    payload,
                    ingested_file,
                    loaded_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    event_ts = EXCLUDED.event_ts,
                    user_id = EXCLUDED.user_id,
                    user_login = EXCLUDED.user_login,
                    repo_source = EXCLUDED.repo_source,
                    event_type = EXCLUDED.event_type,
                    payload = EXCLUDED.payload,
                    ingested_file = EXCLUDED.ingested_file,
                    loaded_at = NOW();
                """,
                (
                    event.get("id"),
                    event.get("created_at"),
                    str(actor.get("id")) if actor.get("id") is not None else None,
                    actor.get("login"),
                    repo.get("name"),
                    event.get("type"),
                    Jsonb(event),
                    source_key,
                ),
            )
            inserted += 1

    conn.commit()
    return inserted

def load_one_object(s3: BaseClient, settings: Settings, key: str) -> list[dict[str, Any]]:
    response = s3.get_object(Bucket=settings.raw_bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue

        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid NDJSON object in {key} at line {line_number}: expected JSON object")
        events.append(payload)

    return events

def get_pg_connection(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )

def run() -> None:
    configure_logging()
    settings = load_settings()
    s3 = build_s3_client(settings)

    logger.info(
        "dwh-loader.started",
        extra={
            "github_org": settings.github_org,
            "raw_bucket": settings.raw_bucket,
            "raw_prefix": settings.raw_prefix,
            "operational_bucket": settings.operational_bucket,
        },
    )

    processed_files = load_checkpoint(s3, settings)
    all_raw_files = list_raw_objects(s3, settings)

    new_files = [key for key in all_raw_files if key not in processed_files]

    logger.info(
        "dwh-loader.files_discovered",
        extra={
            "total_files": len(all_raw_files),
            "new_files": len(new_files),
        },
    )

    if not new_files:
        logger.info("dwh-loader.no_new_files")
        return
    
    conn = get_pg_connection(settings)

    try:
        ensure_raw_table(conn)

        total_events = 0
        for key in new_files:
            events = load_one_object(s3, settings, key)
            loaded = upsert_events(conn, events, key)
            total_events += loaded
            processed_files.add(key)
            save_checkpoint(s3, settings, processed_files)
            logger.info(
                f"dwh-loader.file_loaded. event_count={loaded}",
                extra={
                    "file": key
                },
            )

        logger.info(
            "dwh-loader.completed",
            extra={
                "total_events": total_events,
                "files_processed": len(new_files),
            },
        )
    finally:
        conn.close()

if __name__ == "__main__":
    run()
