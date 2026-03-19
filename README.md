# MarTech Data Platform

This README explains how to set up and run the End-to-End Audience pipeline project on a local machine, step by step.

For architecture and design decisions, see [docs/design.md](docs/design.md).

## 1. What this project does

This project builds a small end-to-end data platform that:

1. extracts GitHub engagement events,
2. stores raw files in MinIO,
3. loads raw events into PostgreSQL,
4. transforms the data with dbt,
5. creates analytics-ready tables and audience tables.

Main modeled outputs:
- `dim_users`
- `dim_repos`
- `dim_event_types`
- `fct_user_repo_engagement`
- `aud_high_intent_users`
- `aud_newly_engaged_users`

## 2. High-level flow

```text
GitHub API -> dl-ingestion -> MinIO raw bucket
MinIO raw bucket -> dwh-loader -> PostgreSQL raw.raw_events
PostgreSQL raw.raw_events -> dbt -> dimensions / facts / audiences
```

## 3. Prerequisites

Install the following before starting:

- Docker Desktop or Docker Engine
- kind
- kubectl
- Python 3.11+
- `psql` client (recommended)

## 4. Clone the repository

```bash
git clone https://github.com/prabhusathy2008/martech-data-platform.git
cd martech-data-platform
```

## 5. Create local secret files

Copy the example files:

```bash
cp platform/k8s/data-platform/minio/minio-secret.env.example platform/k8s/data-platform/minio/minio-secret.env
cp platform/k8s/data-platform/postgres/postgres-secret.env.example platform/k8s/data-platform/postgres/postgres-secret.env
cp platform/k8s/airflow/airflow-secret.env.example platform/k8s/airflow/airflow-secret.env
```

Update the credentials in each copied env file as needed.

## 6. Create the local Kubernetes cluster

```bash
kind create cluster --name martech-local --config infra/kind-local.yaml
```

Verify cluster access:

```bash
kubectl cluster-info --context kind-martech-local
```

## 7. Deploy platform components

Apply all manifests:

```bash
kubectl apply -k platform/k8s
```

Check pod status:

```bash
kubectl get pods -n data-platform
kubectl get pods -n airflow
```

If needed, wait until all pods are ready:

```bash
kubectl get pods -n data-platform -w
```

## 8. Verify services and local endpoints

Check services:

```bash
kubectl get svc -n data-platform
kubectl get svc -n airflow
```

Expected endpoints:

- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001
- PostgreSQL: localhost:5432
- Airflow UI: http://localhost:8080

## 9. Run the pipeline from the Airflow UI

The project includes Airflow DAGs that you will trigger manually in this local setup. In production, these DAGs would run on a schedule.

Open the Airflow UI at http://localhost:8080 and log in with the credentials from [platform/k8s/airflow/airflow-secret.env](platform/k8s/airflow/airflow-secret.env).

Run the DAGs in this order:

1. `dl-ingestion` - pulls source events and writes raw files into MinIO.
2. `dwh-loader` - reads raw files from MinIO and loads them into PostgreSQL `raw.raw_events`.
3. `data-modeling` - runs the modeling pipeline and builds staging, intermediate, mart, and audience models.

For each DAG:
- open the DAG in Airflow,
- click Trigger DAG,
- wait until the DAG run finishes successfully,
- only then trigger the next DAG,
- review task logs if any task fails.

## 10. Explore analytics data

After the pipeline run is complete, use the business SQL queries in [docs/analytics-business-queries.sql](docs/analytics-business-queries.sql) to explore:

- top engaged users
- most engaged repositories
- event type quality mix
- daily engagement trends
- audience composition and overlap

Example:

```bash
psql -h localhost -p 5432 -U <POSTGRES_USER> -d <POSTGRES_DB> -f docs/analytics-business-queries.sql
```

## 11. Cleanup

Delete namespaces:

```bash
kubectl delete namespace data-platform
kubectl delete namespace airflow
```

Delete the kind cluster:

```bash
kind delete cluster --name martech-local
```

## Additional documentation

- [docs/design.md](docs/design.md)
- [docs/runbook.md](docs/runbook.md)
- [docs/audience-logic.md](docs/audience-logic.md)

