from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Modeling is isolated as its own DAG so warehouse load and dbt runtime can evolve independently.
with DAG(
    dag_id="data-modeling",
    description="Run dbt models to transform raw GitHub events into user engagement metrics",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Demo mode: triggered manually after dwh-loader success.
    # schedule="@daily",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["martech", "data-platform", "data-modeling"],
) as dag:
    KubernetesPodOperator(
        task_id="data-modeling",
        name="data-modeling",
        namespace="data-platform",
        image="ghcr.io/prabhusathy2008/martech-data-platform/data-modeling:v1",
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
            k8s.V1EnvVar(name="POSTGRES_PORT", value="5432"),
            k8s.V1EnvVar(
                name="POSTGRES_USER",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="postgres-secret", key="POSTGRES_USER"
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="POSTGRES_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="postgres-secret", key="POSTGRES_PASSWORD"
                    )
                ),
            ),
        ],
    )
