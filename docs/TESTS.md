# Testing Guide

This repository contains two complementary testing approaches:

1) Manual SQL tests that you run yourself in a SQL client
2) Automated assertions that run inside the Airflow DAG and fail the pipeline on breach

---

## 1) Manual SQL tests

Location: `tests/`

- `tests/data_quality_checks_silver.sql`
  - Validates the cleansed Silver layer (duplicates, NULLs, data types, ranges)
- `tests/data_quality_checks_gold.sql`
  - Validates the Gold star schema (referential integrity, completeness, uniqueness, business rules)

How to run (psql example):

```sql
\i path/to/tests/data_quality_checks_silver.sql
\i path/to/tests/data_quality_checks_gold.sql
```

Use this mode when developing transforms or debugging specific tables.

---

## 2) Airflow-enforced assertions

DAG: `dags/postgres_dwh_pipeline.py`

- `check_silver_quality` includes checks such as:
  - No duplicate customers or products
  - No NULL natural keys
- `check_gold_quality` includes checks such as:
  - No NULL surrogate keys in dimensions
  - No orphaned foreign keys in `gold.fact_sales`
  - Data completeness >= 95%

Behavior:

- Any failed assertion raises `AirflowException` and stops the DAG
- The task log shows each test’s name, the SQL executed, and the PASS/FAIL outcome

