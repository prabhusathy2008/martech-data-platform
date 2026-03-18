from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="dl-ingestion",
    description="Ingest GitHub events into MinIO raw bucket (data lake)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    #schedule="@hourly",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["martech", "data-platform", "dl-ingestion"],
) as dag:
    KubernetesPodOperator(
        task_id="dl-ingestion",
        name="dl-ingestion",
        namespace="data-platform",
        image="ghcr.io/prabhusathy2008/martech-data-platform/dl-ingestion:v1",
        image_pull_policy="Always",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name="airflow-task-runner",
        startup_timeout_seconds=300,
        log_events_on_failure=True,
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        env_vars=[
            k8s.V1EnvVar(name="GITHUB_ORG", value="adevinta"),
            k8s.V1EnvVar(name="RAW_BUCKET", value="dl-raw-events"),
            k8s.V1EnvVar(name="OPERATIONAL_BUCKET", value="ops-pipelines"),
            k8s.V1EnvVar(
                name="MINIO_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(name="minio-secret", key="MINIO_ROOT_USER")
                ),
            ),
            k8s.V1EnvVar(
                name="MINIO_SECRET_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(name="minio-secret", key="MINIO_ROOT_PASSWORD")
                ),
            )
        ]
    )