# Airflow Guide

This project includes a production-style orchestration using Apache Airflow with Docker Compose. The DAG executes the full Medallion ETL (Bronze → Silver → Gold) and enforces data quality tests that will fail the pipeline if thresholds are not met.

---

## Stack Overview

- Airflow services: webserver/API, scheduler, triggerer, dag-processor
- Airflow metadata DB: Postgres (container-internal)
- Your data warehouse: Local Postgres on your host machine (outside Docker)
- Volumes:
  - `dags/` → `/opt/airflow/dags`
  - `scripts/` → available as `/opt/airflow/project_root/scripts`
  - `tests/` → available as `/opt/airflow/project_root/tests`
  - `data_sets/` → available under both `/opt/airflow/data_sets` and your Windows path (for Postgres CSV COPY)

---

## Start the stack

- From the repository root:

  - Windows PowerShell: `docker-compose up -d`

- Airflow UI: http://localhost:8080 (user: `airflow`, pass: `airflow`)

---

## Create the warehouse connection

Create a connection to your local Postgres so that the DAG can call stored procedures and run quality queries.

- Admin → Connections → +
  - Conn Id: `postgres_dw`
  - Conn Type: `Postgres`
  - Host: `host.docker.internal` (Docker-to-host bridge on Windows/Mac)
  - Port: `5432`
  - Login: your Postgres user 
  - Password: your password
  - Database: `data_warehouse`

---

## DAG

There is a single DAG in this repository, with enforced data quality assertions:

- DAG id: `sql_dwh_pipeline_with_assertions`

Task flow:

1) `load_bronze` — Calls `bronze.load_bronze(base_path_crm, base_path_erp)`
   - Converts your Windows paths to Postgres-friendly forward slashes
2) `load_silver` — Calls `silver.load_silver()`
3) `check_silver_quality` — Duplicate/NULL checks; fails task on breach
4) `create_gold_views` — Executes `scripts/gold/ddl_gold.sql`
5) `check_gold_quality` — Key integrity and completeness checks; fails task on breach

---

## Reading task logs

- Each assertion prints:
  - `→ Running: <test name>`
  - The SQL that’s executed
  - A PASS/FAIL line, including the actual vs expected value
- Completeness check prints a percentage; the task fails if it is below threshold (95%).

Example failure message:

```
AirflowException: ❌ Data completeness FAILED: 92.10% (expected >= 95%)
```

---


## Stop and clean up

- Stop services: `docker-compose down`
- Prune volumes if needed (this removes Airflow metadata): `docker volume rm $(docker volume ls -q | Select-String postgres-db-volume)`

