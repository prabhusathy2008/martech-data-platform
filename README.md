# MarTech Data Platform

This README explains how to set up and run the full MarTech data platform project on a local machine, step by step.

For architecture and design decisions, see [docs/design.md](docs/design.md)

## 1. What this project does

This project builds a small end-to-end data platform that:

1. extracts GitHub engagement events,
2. stores raw files in MinIO,
3. loads raw events into PostgreSQL,
4. transforms the data with dbt,
5. creates analytics-ready tables and audience tables.

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
- A PostgreSQL-compatible database client (for example, DBeaver, TablePlus, or psql) to explore final data in mart tables

## 4. Clone the repository

```bash
git clone https://github.com/prabhusathy2008/martech-data-platform.git
cd martech-data-platform
```

## 5. Create local secret files

Copy each secret template, create the main secret files, and edit the credentials:

```bash
cp platform/k8s/data-platform/minio/minio-secret.env.example platform/k8s/data-platform/minio/minio-secret.env
cp platform/k8s/data-platform/postgres/postgres-secret.env.example platform/k8s/data-platform/postgres/postgres-secret.env
cp platform/k8s/airflow/airflow-secret.env.example platform/k8s/airflow/airflow-secret.env
```

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

If needed, wait until all pods are ready before continuing.

## 8. Verify services and local endpoints

Check services:

```bash
kubectl get svc -n data-platform
kubectl get svc -n airflow
```

This will generate the following endpoints:

- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001
- PostgreSQL: localhost:5432
- Airflow UI: http://localhost:8080

## 9. Run the pipeline from the Airflow UI

The project includes Airflow DAGs that you will trigger manually in this local setup. In production, these DAGs would run on a schedule.

Open the Airflow UI at http://localhost:8080 and log in with the credentials from [platform/k8s/airflow/airflow-secret.env](platform/k8s/airflow/airflow-secret.env).

Run the DAGs in this order (wait for each DAG run to succeed before triggering the next):

1. `dl-ingestion` - pulls source events and writes raw files into MinIO.
2. `dwh-loader` - reads raw files from MinIO and loads them into PostgreSQL `raw.raw_events`.
3. `data-modeling` - runs the modeling pipeline and builds staging, intermediate, mart, and audience models.

## 10. Explore analytics data

After the pipeline run is complete, connect to PostgreSQL using your preferred DB client and explore the modeled mart outputs.

You can use the example business queries in [docs/analytics-business-queries.sql](docs/analytics-business-queries.sql) as a starting point.

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
- [docs/data-flow-and-env-vars.md](docs/data-flow-and-env-vars.md)
- [docs/analytics-business-queries.sql](docs/analytics-business-queries.sql)