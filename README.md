# AWS_Cloud

## Team
- Leader: Yassir Tagemouati
- Members:
  - Ilyass Bennani
  - Bakr El Asmi
  - Adil Habib

---

## Project Overview
This repository centralizes Infrastructure-as-Code (IaC), automation, and operational utilities for provisioning and operating AWS-based data platforms and pipelines. It includes Airflow DAGs, example scripts to extract and load data (Postgres → S3), dbt orchestration, reusable infrastructure modules, and runbooks that help platform and data engineering teams deploy and maintain cloud resources in a repeatable and auditable way.

Primary goals:
- Provide reusable infrastructure modules and examples for new services
- Automate data ingestion, transformation, and deployment pipelines
- Enforce security and operational best practices for cloud resources

Primary languages & tools:
- Python (Airflow DAGs, scripts)
- Terraform / CloudFormation / CDK (expected under infra/)
- dbt (transformations)
- AWS (S3, IAM, RDS, Lambda, etc.)

---

## Repository Structure
(Top-level overview — individual sub-projects should contain their own README.md)
- infra/                — Terraform / CloudFormation / CDK projects and modules (per-environment)
- modules/              — Reusable IaC modules
- StreamVisionTP/
  - airflow/dags/       — Airflow DAGs and DAG-specific helper scripts
  - scripts/            — Standalone scripts (export_to_s3.py, data generators, helpers)
  - dbt/                — dbt project (if present)
- scripts/              — Shared helper scripts and utilities
- examples/             — Example usage and starter stacks
- docs/                 — Operational runbooks, architecture notes, runbooks
- ci/                   — CI/CD configuration and pipeline definitions
- README.md             — This file
- LICENSE               — Add a license file (recommended)

Note: Some files may be duplicated for convenience (e.g., DAG-embedded scripts and top-level scripts). See "Maintenance" below for recommendations.

---

## Quick Start (developer / local)
These instructions get a developer set up to run and test the pipeline locally or in a development environment.

Prerequisites
- Git
- Python 3.8+ (match your Airflow/dbt runtime)
- pip (or poetry/poetry)
- Docker (optional but recommended for local Postgres/Airflow)
- AWS CLI configured (profiles or environment variables)
- Terraform >= 1.0 (if you plan to deploy infra via Terraform)
- dbt (if you use dbt transformations)

Recommended steps
1. Clone the repo:
   ```bash
   git clone https://github.com/TageYassir/AWS_Cloud.git
   cd AWS_Cloud
   ```
2. Create a Python virtual environment and install dependencies (adjust as needed):
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # if requirements exist
   ```
3. Create a local configuration file:
   - Copy the .env.template to .env and fill in values (see the "Configuration" section).
   ```bash
   cp .env.template .env
   # edit .env with secure values
   ```
4. Run the export script locally (sanity test):
   ```bash
   python StreamVisionTP/scripts/export_to_s3.py
   ```
   - The script will attempt to connect to the configured Postgres and S3. Use Docker to start a local Postgres for testing or point to a dev database.

---

## Configuration & Secrets (IMPORTANT)
Never store secrets (passwords, access keys, tokens) in the repository. Use environment variables, AWS IAM roles, or a secrets manager (AWS Secrets Manager / SSM parameter store). The repository contains example placeholders that must be replaced before use.

Recommended environment variables
- DB_HOST
- DB_NAME
- DB_USER
- DB_PASSWORD
- S3_BUCKET
- AWS_REGION
- AWS_PROFILE (optional, for named AWS CLI profile)
- SLACK_WEBHOOK_URL (optional)
- DBT_PROJECT_DIR (path to dbt project)
- AIRFLOW_HOME (if running Airflow locally)

Sample .env.template (place at repo root)
```text
# Database
DB_HOST=host.docker.internal
DB_NAME=streamvision
DB_USER=postgres
DB_PASSWORD=change_me

# AWS
AWS_REGION=eu-north-1
AWS_PROFILE=default
S3_BUCKET=streamvision-data-raw

# Slack
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXXX/YYYYY/ZZZZZ

# dbt
DBT_PROJECT_DIR=/path/to/dbt/project
```

How scripts should read secrets
- Prefer built-in credential resolution:
  - boto3 will use the AWS SDK default chain (environment vars, shared credentials file, IAM role).
  - For DB credentials, read from environment variables or secrets manager.
- Example Python pattern (pseudo):
  ```py
  import os
  DB_CONFIG = {
      "host": os.environ["DB_HOST"],
      "database": os.environ.get("DB_NAME", "streamvision"),
      "user": os.environ["DB_USER"],
      "password": os.environ["DB_PASSWORD"],
  }
  ```

Secrets management options
- AWS Secrets Manager / Parameter Store (recommended for production)
- Environment variables with secure provisioning in CI/CD
- GitHub Actions secrets + OIDC-based role assumption (recommended over static access keys)

---

## How the StreamVision daily pipeline works (high-level)
- Extraction: A Python script connects to Postgres, reads tables into pandas DataFrames, and writes CSV files to S3 in raw/postgres/<table>/<date>/...
- Orchestration: Airflow DAG orchestrates extraction, waits for S3 keys via S3KeySensor, loads into staging, triggers dbt runs, generates dbt docs, runs dbt tests, and posts a Slack notification.
- Transformations: dbt runs staging → core → marts models and runs tests.
- Observability: Logs are emitted by Airflow and the invoked scripts; add CloudWatch or other logging sinks as required.

Important file locations
- DAG: StreamVisionTP/airflow/dags/streamvision_daily_pipeline.py
- Export script(s): StreamVisionTP/scripts/export_to_s3.py and StreamVisionTP/airflow/dags/scripts/export_to_s3.py (deduplicate if possible)
- Data generators: StreamVisionTP/scripts/generate_streaming_data.py

---

## Running the Airflow DAG locally
1. Install Airflow (constrain provider versions to match imports in DAG).
2. Ensure `AIRFLOW_HOME` is set and init DB:
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   ```
3. Place DAG files under `$AIRFLOW_HOME/dags` or configure to mount repository path.
4. Start scheduler and webserver:
   ```bash
   airflow scheduler &
   airflow webserver --port 8080
   ```
5. Trigger DAG via UI or CLI:
   ```bash
   airflow dags trigger streamvision_daily_pipeline
   ```

Notes:
- The DAG currently runs PythonOperator tasks that call subprocess to execute scripts. For better testability and observability, refactor to import functions and use Airflow hooks (PostgresHook, S3Hook) where appropriate.

---

## dbt usage
- To run models:
  ```bash
  dbt run --project-dir /path/to/dbt/project
  ```
- To run tests:
  ```bash
  dbt test --project-dir /path/to/dbt/project
  ```
- To generate docs:
  ```bash
  dbt docs generate --project-dir /path/to/dbt/project
  ```

Ensure dbt profiles are configured with credentials (do not store them in repo).

---


## Security & Best Practices (mandatory)
1. Remove secrets from code:
   - Replace any hardcoded credentials (e.g., `"password": "1234"`) with secure retrieval mechanisms.
2. Use least privilege IAM roles:
   - Create fine-grained IAM policies and do not attach broad permissions to service roles.
3. Secure S3:
   - Enable encryption at rest (SSE-KMS), restrict public access, and enable bucket policies and access logging.
4. Use separate AWS accounts/environments for dev/staging/prod.
5. Enable monitoring & detection:
   - CloudWatch logs/metrics, AWS Config, GuardDuty, and alerts.
6. Add secret scanning:
   - Add a GitHub Action or pre-commit hook to scan for secrets (detect-secrets, truffleHog).
7. Rotate credentials and audit access regularly.
8. Document all privileged IAM roles and service accounts.

---

## Recommended Maintenance & Refactors
- Deduplicate scripts:
  - Consolidate `export_to_s3.py` into a single module under `StreamVisionTP/scripts/` and import this module from the DAG instead of invoking via subprocess.
- Replace subprocess calls with Airflow-native hooks/operators:
  - Use `PostgresHook`, `S3Hook`, and `PythonOperator` with imported callables for better logging and retries.
- Add automated tests:
  - Unit tests for export logic (mock Postgres and S3), integration tests using localstack or ephemeral test infra.
- Add linting & formatting:
  - Enforce `black`, `ruff`/`flake8` in CI.
- Add a GitHub Actions workflow:
  - CI: lint → unit tests → secret-scan
  - CD: Terraform plan & apply with gated approvals

---

## CI/CD suggestions
- Add workflows:
  - ci.yml: runs lint, unit tests, and secret scanners on PRs
  - infra-ci.yml: runs `terraform plan` in PRs and `terraform apply` on protected branches using GitHub Actions with OIDC roles
- Use GitHub Environments and required approvers for production deployments.

---

## Issues & Roadmap ideas
- Replace hardcoded DB/S3 credentials with secure secret retrieval
- Consolidate duplicated scripts and refactor DAGs to import functions
- Add automated secret scanning in CI
- Add full infra examples under infra/<project> with per-environment variables
- Add unit & integration tests (dbt, export scripts)

---

## Troubleshooting & Runbook (brief)
- If export script fails to connect to Postgres:
  - Verify DB_HOST, DB_USER, DB_PASSWORD are set and reachable
  - If running in Docker, verify network and host mapping (host.docker.internal behaves differently on Linux)
- If S3 upload fails:
  - Verify AWS credentials and permissions for the S3 bucket and region
  - Check bucket existence, encryption policies, and bucket ACLs
- If Airflow DAG fails:
  - Inspect Airflow logs in web UI for the task instance
  - Increase logging and add retries/timeouts in DAG task definitions

