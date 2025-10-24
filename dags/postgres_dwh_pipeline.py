"""
SQL Data Warehouse ETL Pipeline — WITH DATA QUALITY ASSERTIONS

Overview
--------
This DAG orchestrates the Medallion ETL (Bronze → Silver → Gold) and ENFORCES
data quality rules. Any failed assertion raises AirflowException and stops the
pipeline.

Prerequisites (one‑time bootstrap executed directly on your local Postgres):
    1) Run scripts/00_init_database.sql (creates DB + schemas)
    2) Run scripts/bronze/ddl_bronze.sql and scripts/bronze/proc_load_bronze.sql
    3) Run scripts/silver/ddl_silver.sql and scripts/silver/proc_load_silver.sql

Airflow connection required:
    - Conn Id: postgres_dw
    - Type: Postgres, Host: host.docker.internal, Port: 5432
    - Database: datawarehouse, Login/Password: your local credentials

CSV file access:
    - The Bronze loader reads CSVs directly from your Windows paths via COPY.
    - The DAG passes absolute Windows paths (converted to forward slashes) to
        bronze.load_bronze(base_path_crm, base_path_erp).

Author: SouTa9
Repository: https://github.com/SouTa9/SQL-Data-Warehouse-Project
"""

from __future__ import annotations
import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


# ================================
# Configuration Constants
# ================================

POSTGRES_CONN_ID = "postgres_dw"

# Windows paths for local PostgreSQL to access CSV files
WINDOWS_PROJECT_ROOT = (
    r"C:\Users\vlado\Desktop\SQL-project-last\SQL-Data-Warehouse-Project"
)
WINDOWS_DATASETS_PATH = os.path.join(WINDOWS_PROJECT_ROOT, "data_sets")
CRM_PATH = os.path.join(WINDOWS_DATASETS_PATH, "source_crm")
ERP_PATH = os.path.join(WINDOWS_DATASETS_PATH, "source_erp")

# Container paths for Airflow to read SQL script files
CONTAINER_PROJECT_ROOT = "/opt/airflow/project_root"
SQL_SCRIPTS_DIR = os.path.join(CONTAINER_PROJECT_ROOT, "scripts")
SQL_TESTS_DIR = os.path.join(CONTAINER_PROJECT_ROOT, "tests")


# ================================
# Helper Functions
# ================================


def read_sql_file(file_path: str) -> str:
    """Read SQL script content from a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            sql_content = file.read()
            print(f"✓ Successfully loaded: {os.path.basename(file_path)}")
            return sql_content
    except FileNotFoundError:
        error_msg = f"✗ CRITICAL: SQL file not found at {file_path}"
        print(error_msg)
        raise FileNotFoundError(error_msg)


def run_quality_assertion(pg_hook, test_name: str, query: str, expected_value: int = 0):
    """
    Run a data quality assertion and fail if threshold is exceeded.

    Args:
        pg_hook: PostgreSQL hook
        test_name: Human-readable test name
        query: SQL query that returns a single numeric value
        expected_value: Expected result (usually 0 for violation checks)

    Raises:
        AirflowException: If quality check fails
    """
    print(f"  → Running: {test_name}")
    result = pg_hook.get_first(query)
    actual_value = result[0] if result else None

    if actual_value is None:
        raise AirflowException(f"❌ {test_name}: Query returned NULL")

    if actual_value != expected_value:
        raise AirflowException(
            f"❌ {test_name} FAILED: Expected {expected_value}, got {actual_value}"
        )

    print(f"    ✓ PASS: {actual_value} (expected: {expected_value})")


# ================================
# DAG Definition
# ================================


@dag(
    dag_id="sql_dwh_pipeline_with_assertions",
    start_date=pendulum.datetime(2025, 10, 22, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["medallion", "datawarehouse", "etl", "quality-enforced"],
    description="ETL pipeline with enforced data quality assertions",
)
def sql_data_warehouse_pipeline_with_assertions():
    """
    ETL pipeline with ENFORCED data quality checks.
    Tasks will FAIL if quality thresholds are not met.
    """

    @task(task_id="load_bronze")
    def load_bronze_task():
        """Load raw data from CSV files into Bronze layer tables."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Convert Windows paths to PostgreSQL-friendly format
        crm_path_postgres = CRM_PATH.replace("\\", "/") + "/"
        erp_path_postgres = ERP_PATH.replace("\\", "/") + "/"

        sql_command = (
            f"CALL bronze.load_bronze('{crm_path_postgres}', '{erp_path_postgres}');"
        )

        print("📦 Executing Bronze Layer Load...")
        print(f"   CRM Path: {crm_path_postgres}")
        print(f"   ERP Path: {erp_path_postgres}")

        pg_hook.run(sql_command)
        print("✓ Bronze layer loaded successfully")

    @task(task_id="load_silver")
    def load_silver_task():
        """Transform Bronze data into cleansed Silver layer tables."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql_command = "CALL silver.load_silver();"

        print("🔄 Executing Silver Layer Transformation...")
        pg_hook.run(sql_command)
        print("✓ Silver layer loaded successfully")

    @task(task_id="check_silver_quality")
    def check_silver_quality_task():
        """
        Run ENFORCED data quality checks on Silver layer.
        Task will FAIL if quality issues exceed thresholds.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        print("🔍 Executing Silver Layer Quality Assertions...")
        print("=" * 60)

        # Critical assertions that must pass
        run_quality_assertion(
            pg_hook,
            "No duplicate customers in silver.crm_cust_info",
            """
            SELECT COUNT(*) FROM (
                SELECT cst_id, COUNT(*) 
                FROM silver.crm_cust_info 
                GROUP BY cst_id 
                HAVING COUNT(*) > 1
            ) duplicates;
            """,
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No NULL customer IDs in silver.crm_cust_info",
            "SELECT COUNT(*) FROM silver.crm_cust_info WHERE cst_id IS NULL;",
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No duplicate products in silver.crm_prd_info",
            """
            SELECT COUNT(*) FROM (
                SELECT prd_id, COUNT(*) 
                FROM silver.crm_prd_info 
                GROUP BY prd_id 
                HAVING COUNT(*) > 1
            ) duplicates;
            """,
            expected_value=0,
        )

        print("=" * 60)
        print("✓ All Silver quality assertions passed")

    @task(task_id="create_gold_views")
    def create_gold_views_task():
        """Create Gold layer views for the dimensional model."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        gold_file_path = os.path.join(SQL_SCRIPTS_DIR, "gold", "ddl_gold.sql")
        sql_script = read_sql_file(gold_file_path)

        print("⭐ Creating Gold Layer Views...")
        pg_hook.run(sql_script)
        print("✓ Gold layer views created successfully")

    @task(task_id="check_gold_quality")
    def check_gold_quality_task():
        """
        Run ENFORCED data quality checks on Gold layer.
        Task will FAIL if quality issues exceed thresholds.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        print("🔍 Executing Gold Layer Quality Assertions...")
        print("=" * 60)

        # Critical assertions for Gold layer
        run_quality_assertion(
            pg_hook,
            "No NULL customer keys in dim_customers",
            "SELECT COUNT(*) FROM gold.dim_customers WHERE customer_key IS NULL;",
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No duplicate customers in dim_customers",
            """
            SELECT COUNT(*) FROM (
                SELECT customer_id, COUNT(*) 
                FROM gold.dim_customers 
                GROUP BY customer_id 
                HAVING COUNT(*) > 1
            ) duplicates;
            """,
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No NULL product keys in dim_products",
            "SELECT COUNT(*) FROM gold.dim_products WHERE product_key IS NULL;",
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No orphaned products in fact_sales",
            """
            SELECT COUNT(*) 
            FROM gold.fact_sales f
            WHERE f.product_key NOT IN (
                SELECT product_key 
                FROM gold.dim_products 
                WHERE product_key IS NOT NULL
            );
            """,
            expected_value=0,
        )

        run_quality_assertion(
            pg_hook,
            "No orphaned customers in fact_sales",
            """
            SELECT COUNT(*) 
            FROM gold.fact_sales f
            WHERE f.customer_key NOT IN (
                SELECT customer_key 
                FROM gold.dim_customers 
                WHERE customer_key IS NOT NULL
            );
            """,
            expected_value=0,
        )

        # Check data completeness (allow up to 5% incomplete records)
        print("  → Checking: Data completeness (must be >= 95%)")
        result = pg_hook.get_first(
            """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN product_key IS NOT NULL AND customer_key IS NOT NULL THEN 1 ELSE 0 END) 
                / NULLIF(COUNT(*), 0), 
                2
            ) AS completeness_percentage
            FROM gold.fact_sales;
        """
        )
        completeness = result[0] if result else 0

        if completeness < 95.0:
            raise AirflowException(
                f"❌ Data completeness FAILED: {completeness}% (expected >= 95%)"
            )

        print(f"    ✓ PASS: {completeness}% completeness (threshold: >= 95%)")

        print("=" * 60)
        print("✓ All Gold quality assertions passed")

    # Task Dependencies
    bronze = load_bronze_task()
    silver = load_silver_task()
    silver_qc = check_silver_quality_task()
    gold = create_gold_views_task()
    gold_qc = check_gold_quality_task()

    bronze >> silver >> silver_qc >> gold >> gold_qc


# Instantiate the DAG
sql_data_warehouse_pipeline_with_assertions()
