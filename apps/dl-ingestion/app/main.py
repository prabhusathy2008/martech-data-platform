import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3
import requests
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    github_token: str | None
    raw_bucket: str
    raw_prefix: str
    operational_bucket: str
    checkpoint_key: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    github_request_timeout_seconds: int

def load_settings() -> Settings:
    org = os.getenv("GITHUB_ORG", "adevinta")

    # Running inside cluster: use service DNS name.
    # Running locally from laptop: use NodePort exposed on localhost.
    default_minio_endpoint = (
        "http://minio:9000"
        if os.getenv("KUBERNETES_SERVICE_HOST")
        else "http://localhost:9000"
    )

    return Settings(
        github_org=org,
        github_token=os.getenv("GITHUB_TOKEN") or None,
        raw_bucket=os.getenv("RAW_BUCKET", "dl-raw-events"),
        raw_prefix=os.getenv("RAW_PREFIX", "source=github"),
        operational_bucket=os.getenv("OPERATIONAL_BUCKET", "ops-pipelines"),
        checkpoint_key=os.getenv("CHECKPOINT_KEY", f"dl-ingestion/events/github/{org}/checkpoint.json"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", default_minio_endpoint),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY"),
        github_request_timeout_seconds=int(os.getenv("GITHUB_REQUEST_TIMEOUT_SECONDS", "30")),
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

def load_checkpoint(s3: BaseClient, settings: Settings) -> str | None:
    """Return last processed event ID from the operational bucket, or None."""
    try:
        response = s3.get_object(
            Bucket=settings.operational_bucket,
            Key=settings.checkpoint_key,
        )
        data: dict[str, Any] = json.loads(response["Body"].read())
        last_id: str | None = data.get("last_processed_id")
        return last_id
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404", "NoSuchBucket"):
            return None
        raise

def save_checkpoint(s3: BaseClient, settings: Settings, last_id: str) -> None:
    """Persist the latest processed event ID to the operational bucket."""
    checkpoint: dict[str, Any] = {
        "last_processed_id": last_id,
        "last_run": datetime.now(UTC).isoformat(),
    }
    s3.put_object(
        Bucket=settings.operational_bucket,
        Key=settings.checkpoint_key,
        Body=json.dumps(checkpoint).encode("utf-8"),
        ContentType="application/json",
    )

def write_events_to_raw(
    s3: BaseClient,
    events: list[dict[str, Any]],
    settings: Settings,
) -> str:
    """Write a batch of events as a single NDJSON file to the raw bucket."""
    now = datetime.now(UTC)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")

    # Deterministic key for idempotency:
    # on retries before checkpoint update, the same batch rewrites same object.
    latest_id = str(events[0]["id"])
    oldest_id = str(events[-1]["id"])
    object_key = (
        f"{settings.raw_prefix}/org={settings.github_org}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"events-latest-{latest_id}-oldest-{oldest_id}.ndjson"
    )
    body = "\n".join(json.dumps(event, separators=(",", ":")) for event in events) + "\n"
    s3.put_object(
        Bucket=settings.raw_bucket,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return object_key

def _build_retrying_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def _build_headers(settings: Settings) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "martech-dl-ingestion/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    return headers

def _next_page_url(link_header: str) -> str | None:
    """Parse GitHub's Link header and return the 'next' URL if present."""
    for part in link_header.split(","):
        segments = part.strip().split(";")
        if len(segments) == 2 and segments[1].strip() == 'rel="next"':
            return segments[0].strip().strip("<>")
    return None

def _poll_interval_seconds(response: requests.Response) -> int | None:
    """Return GitHub's suggested poll interval in seconds, if present and valid."""
    value = response.headers.get("X-Poll-Interval")
    if value is None:
        return None
    try:
        interval = int(value)
    except ValueError:
        logger.warning("dl-ingestion.invalid_poll_interval", extra={"value": value})
        return None
    return interval

def fetch_new_events(
    settings: Settings,
    last_processed_id: str | None,
) -> list[dict[str, Any]]:
    """
    Fetch events from the GitHub org events API.

    - If last_processed_id is None  → return all available events.
    - Otherwise                     → return only events whose ID is
                                      strictly greater than last_processed_id.

    Events are returned in the order delivered by the API (newest first).
    Pagination is followed automatically via the Link header.
    """
    headers = _build_headers(settings)
    session = _build_retrying_session()
    url: str | None = (
        f"https://api.github.com/orgs/{settings.github_org}/events?per_page=100"
    )
    collected: list[dict[str, Any]] = []
    watermark = int(last_processed_id) if last_processed_id is not None else None

    try:
        while url:
            response = session.get(
                url,
                headers=headers,
                timeout=settings.github_request_timeout_seconds,
            )

            if response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0":
                reset_epoch = int(response.headers.get("X-RateLimit-Reset", "0"))
                sleep_seconds = max(reset_epoch - int(time.time()), 0)
                raise RuntimeError(
                    f"GitHub API rate limit exceeded. Retry after ~{sleep_seconds} seconds."
                )

            response.raise_for_status()
            events: list[dict[str, Any]] = response.json()

            if not events:
                break

            done = False
            for event in events:
                event_id = int(str(event["id"]))
                if watermark is not None and event_id <= watermark:
                    done = True
                    break
                collected.append(event)

            if done:
                break

            next_url = _next_page_url(response.headers.get("Link", ""))
            poll_interval_seconds = _poll_interval_seconds(response)
            if next_url and poll_interval_seconds:
                logger.info(
                    "dl-ingestion.poll_interval_wait",
                    extra={
                        "seconds": poll_interval_seconds,
                        "next_url": next_url,
                    },
                )
                time.sleep(poll_interval_seconds)

            url = next_url
    finally:
        session.close()

    return collected

def run() -> None:
    configure_logging()
    settings = load_settings()
    s3 = build_s3_client(settings)

    logger.info(
        "dl-ingestion.started",
        extra={
            "github_org": settings.github_org,
            "raw_bucket": settings.raw_bucket,
            "raw_prefix": settings.raw_prefix,
            "operational_bucket": settings.operational_bucket,
        },
    )

    last_processed_id = load_checkpoint(s3, settings)

    if last_processed_id:
        logger.info("dl-ingestion.checkpoint_found", extra={"last_processed_id": last_processed_id})
    else:
        logger.info("dl-ingestion.checkpoint_missing")

    events = fetch_new_events(settings, last_processed_id)

    if not events:
        logger.info("dl-ingestion.no_new_events")
        return

    object_key = write_events_to_raw(s3, events, settings)
    logger.info(
        "dl-ingestion.raw_written",
        extra={
            "event_count": len(events),
            "bucket": settings.raw_bucket,
            "object_key": object_key,
        },
    )

    # Events are newest-first; index 0 holds the most recent ID.
    latest_id: str = events[0]["id"]
    save_checkpoint(s3, settings, latest_id)
    logger.info("dl-ingestion.checkpoint_updated", extra={"last_processed_id": latest_id})

if __name__ == "__main__":
    run()
