# Runbook: MarTech Data Platform

## 1) Purpose
Operational guide for running, validating, and recovering the local end-to-end pipeline.

## 2) Prerequisites
- Docker is running
- kind, kubectl
- Python 3.11+
- Access to GitHub Container Registry if pushing images

## 3) Standard Workflow
1. Create the cluster and deploy platform manifests.
2. Run the ingestion job (API -> MinIO).
3. Run the loader job (MinIO -> PostgreSQL raw).
4. Run the dbt job (transform -> marts/audiences).
5. Validate outputs and dbt tests.

## 4) Kubernetes Operations
### Create cluster
```bash
kind create cluster --name martech-local --config infra/kind-local.yaml
kubectl cluster-info --context kind-martech-local
```

### Deploy / refresh platform
```bash
kubectl apply -k platform/k8s
```

### Check health
```bash
kubectl get pods -n data-platform
kubectl get pods -n airflow
```

### Run jobs manually

#### dl-ingestion
```bash
kubectl delete job -n data-platform dl-ingestion-manual --ignore-not-found
kubectl apply -f apps/dl-ingestion/dl-ingestion-job.yaml
kubectl logs -n data-platform -f job/dl-ingestion-manual
```

#### dwh-loader
```bash
kubectl delete job -n data-platform dwh-loader-manual --ignore-not-found
kubectl apply -f apps/dwh-loader/dwh-loader-job.yaml
kubectl logs -n data-platform -f job/dwh-loader-manual
```

#### data-modeling
```bash
kubectl delete job -n data-platform data-modeling-manual --ignore-not-found
kubectl apply -f apps/data-modeling/data-modeling-job.yaml
kubectl logs -n data-platform -f job/data-modeling-manual
```

### Teardown
```bash
kubectl delete namespace data-platform
kubectl delete namespace airflow
kind delete cluster --name martech-local
```

## 5) Common Incidents
### A) dbt fails: `relation raw.raw_events does not exist`
- Cause: a fresh database without bootstrap DDL.
- Fix: execute `apps/dwh-loader/app/sql/raw_raw_events.sql` before the dbt run.

### B) GHCR push fails: `permission_denied: write_package`
- Confirm workflow/job has `packages: write`.
- Confirm package permissions grant repo write/admin access.
- Use PAT (`GHCR_TOKEN`) if `GITHUB_TOKEN` scope is restricted.

### C) Loader runs but no rows appear
- Verify MinIO object path and bucket configuration.
- Check loader logs for JSON parsing or database connectivity errors.
- Re-run the loader job after fixing environment variables.

### D) Audience tables empty unexpectedly
- Verify source fact has recent data.
- Check suppression seed for over-filtering.
- Validate threshold/window variables in the dbt run.