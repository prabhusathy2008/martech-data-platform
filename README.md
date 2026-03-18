# MarTech Data Platform (Assignment Scaffold)

This repository is structured for a local, reproducible MarTech data platform on kind + Kubernetes.

## Project layout

- `infra`: local cluster/bootstrap configs (e.g., kind config)
- `platform/k8s/kustomization.yaml`: single kustomize entrypoint
- `platform/k8s/data-platform`: namespace, MinIO, and PostgreSQL manifests
- `platform/k8s/airflow`: namespace, service account, deployment, and service manifests
- `platform/k8s/airflow-data-platform-binding.yaml`: cross-namespace Role + RoleBinding for Airflow task pods
- `airflow-dags`: local DAGs folder mounted into Airflow at `/opt/airflow/dags`
- `apps`: application modules (dl-ingestion, dwh-load, data-modeling)
- `data/sql`: schema and seed scripts
- `docs`: architecture notes and runbook
- `tests`: smoke/integration tests (next step)

## Quick start

From repository root:

1. Create the kind cluster
	- `kind create cluster --name martech-local --config infra/kind-local.yaml`

2. Create local credential files from the provided examples and edit with real values
	```bash
	cp platform/k8s/data-platform/minio/minio-secret.env.example platform/k8s/data-platform/minio/minio-secret.env
	cp platform/k8s/data-platform/postgres/postgres-secret.env.example platform/k8s/data-platform/postgres/postgres-secret.env
	cp platform/k8s/airflow/airflow-secret.env.example platform/k8s/airflow/airflow-secret.env
	# Then edit each file and set your own values
	```

3. Deploy platform resources
	- `kubectl apply -k platform/k8s`

4. Verify deployment
	- `kubectl get pods -n data-platform`
	- `kubectl get pods -n airflow`
    - `kubectl get pods --all-namespaces | grep -E "data-platform|airflow"`
	- `kubectl get svc -n data-platform`
    - `kubectl get svc --all-namespaces | grep -E "data-platform|airflow"`

### Local access

- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001
- PostgreSQL: localhost:5432
- Airflow UI: http://localhost:8080

### Cleanup

- `kubectl delete namespace data-platform`
- `kubectl delete namespace airflow`
- `kind delete cluster --name martech-local`
