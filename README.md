# SQL Data Warehouse Project

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)](https://www.postgresql.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)](https://www.databricks.com/glossary/medallion-architecture)

## 📊 Overview

End-to-end data warehouse implementing the **Medallion Architecture** (Bronze → Silver → Gold). Integrates CRM and ERP
data sources into a unified analytics platform with dimensional modeling for business intelligence. The project can run
manually via SQL scripts or be orchestrated with **Apache Airflow** (Docker Compose) with enforced data quality checks.

**Key Features:**

- Full ETL pipeline with automated, enforced quality checks (Airflow)
- Three-tier architecture (Bronze → Silver → Gold)
- Star schema with 2 dimensions and 1 fact table
- 35+ automated data quality tests
- PostgreSQL stored procedures for data transformations

---

## 🏗️ Architecture

### Medallion Layers

| Layer      | Purpose              | Data State        | Tables/Views |
|------------|----------------------|-------------------|--------------|
| **Bronze** | Raw data ingestion   | Unprocessed       | 6 tables     |
| **Silver** | Cleansed & validated | Standardized      | 6 tables     |
| **Gold**   | Business analytics   | Dimensional model | 3 views      |

### Data Flow

```
CSV Files (CRM/ERP) → Bronze Layer → Silver Layer → Gold Layer (Star Schema)
                      (Raw data)    (Cleansed)     (2 Dims + 1 Fact)
```

---

## 📁 Project Structure

```
SQL-Data-Warehouse-Project/
├── docker-compose.yaml         # Local Airflow stack (web, scheduler, metadata DB)
├── dags/                       # Airflow DAGs (basic + with assertions)
│   ├── postgres_dwh_pipeline.py
│   └── postgres_dwh_pipeline_with_assertions.py
├── data_sets/
│   ├── source_crm/             # CRM: customers, products, sales (~60K records)
│   └── source_erp/             # ERP: demographics, locations, categories
├── scripts/
│   ├── 00_init_database.sql    # Database initialization
│   ├── bronze/                 # Raw data ingestion (DDL + procedures)
│   ├── silver/                 # Data transformation (DDL + procedures)
│   └── gold/                   # Star schema views (2 dims, 1 fact)
├── tests/                      # Manual SQL tests (Silver/Gold)
└── docs/                       # Setup, Airflow, testing, data catalog
```

---

## 🗄️ Database Schema

### Gold Layer - Star Schema (Ready for BI)

| Object         | Type      | Description                        | Records |
|----------------|-----------|------------------------------------|---------|
| `dim_customers` | Dimension | Customer master (CRM + ERP merged) | ~700    |
| `dim_products` | Dimension | Active products with categories    | ~200    |
| `fact_sales`   | Fact      | Sales transactions with metrics    | ~60K    |

**Bronze & Silver Layers:** See [docs/data_catalog.md](docs/data_catalog.md) for detailed schemas.

---

## 🚀 Quick Start

You can run the project with Airflow (recommended for orchestration/monitoring) or manually via SQL. Airflow requires a one‑time manual bootstrap to create the database, schemas, and procedures.

### Option A — Run with Airflow (Docker, recommended)

0) One‑time bootstrap in your local Postgres (outside Docker)
     - Create DB and schemas:
         - Run `scripts/00_init_database.sql` from a superuser (creates the `data_warehouse` DB and bronze/silver/gold schemas)
     - Create tables and procedures (no data yet):
         - Run `scripts/bronze/ddl_bronze.sql`
         - Run `scripts/bronze/proc_load_bronze.sql`
         - Run `scripts/silver/ddl_silver.sql`
         - Run `scripts/silver/proc_load_silver.sql`

1) Start the stack
    - Install Docker Desktop, then from the repo root: `docker-compose up -d`
2) Create Airflow connection to your local Postgres (warehouse)
    - UI http://localhost:8080 (user/pass: airflow/airflow)
    - Admin → Connections → +
      - Conn Id: `postgres_dw`
      - Conn Type: Postgres
      - Host: `host.docker.internal`
      - Port: `5432`
      - Login: your Postgres user (e.g., `bootcamp_admin`)
      - Password: your password
      - Database: `datawarehouse`
3) Trigger the DAG (single DAG): `sql_dwh_pipeline_with_assertions`
    - Executes: Bronze → Silver → Silver assertions → Gold views → Gold assertions
    - Any failed check stops the DAG with a clear error message

### Option B — Manual SQL Run (works end‑to‑end without Airflow)

```bash
# 1. Initialize database and schemas
psql -U postgres -f scripts/00_init_database.sql

# 2. Connect to the warehouse
psql -U postgres -d data_warehouse

# 3. Create and load Bronze layer (pass your CSV base paths)
\i scripts/bronze/ddl_bronze.sql
-- Example (PowerShell-friendly forward slashes):
-- CALL bronze.load_bronze('C:/path/to/data_sets/source_crm/','C:/path/to/data_sets/source_erp/');

# 4. Create and load Silver layer
\i scripts/silver/ddl_silver.sql
CALL silver.load_silver();

# 5. Create Gold layer views
\i scripts/gold/ddl_gold.sql

# 6. Run manual data quality checks
\i tests/data_quality_checks_silver.sql
\i tests/data_quality_checks_gold.sql
```

**Note:** Update file paths in `scripts/bronze/proc_load_bronze.sql` before running step 3.

---

## 🔄 Key Transformations

### Bronze → Silver

- **Deduplication:** ROW_NUMBER() window functions
- **Standardization:** Gender (M→Male), Marital Status (M→Married), Product Lines (R→Road)
- **Data Type Conversion:** INT dates (YYYYMMDD) → DATE
- **Data Quality:** NULL handling, trimming, validation

### Silver → Gold

- **Dimension Integration:** Multi-source joins (CRM + ERP)
- **Surrogate Keys:** ROW_NUMBER() for optimized joins
- **Business Logic:** Gender priority (CRM over ERP), active products only
- **Star Schema:** Fact table with dimension foreign keys

---

## 🛠️ Technical Highlights

### SQL Techniques

- **Window Functions:** ROW_NUMBER(), LEAD() for deduplication and SCD Type 2
- **CTEs & Subqueries:** Multi-step transformations
- **Stored Procedures:** Automated ETL with error handling and logging
- **Date Functions:** INT to DATE conversions (YYYYMMDD format)

### Data Quality

- Manual tests: `tests/data_quality_checks_silver.sql`, `tests/data_quality_checks_gold.sql`
- Airflow‑enforced assertions (same rules, automated): `dags/postgres_dwh_pipeline.py`
    - DAG id: `sql_dwh_pipeline_with_assertions`
    - Failing checks raise AirflowException and stop the DAG

### Why Airflow vs Manual?

- Scheduling and retries: run on a schedule with built‑in backoff and retries
- Observability: task logs, durations, and clear PASS/FAIL per assertion
- Orchestration: consistent ordering Bronze → Silver → Gold → checks
- Parameters and portability: uses your Windows base paths and project‑mounted SQL scripts
- Manual mode is perfect for development/debugging and gives you fine‑grained control; Airflow adds production‑style reliability and visibility

### Performance

- COPY command for bulk CSV loading
- Surrogate keys for optimized joins
- View-based Gold layer for real-time analytics

---

## 🎯 Skills Demonstrated

- ETL pipeline development with PostgreSQL
- Medallion Architecture implementation
- Dimensional modeling (Star Schema)
- Advanced SQL (window functions, CTEs, stored procedures)
- Data quality validation and testing
- Multi-source data integration

---

## 📊 Sample Analytics

```sql
-- Top customers by revenue
SELECT c.firstname, c.lastname, SUM(f.sales) as revenue
FROM gold.fact_sales f
         JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.firstname, c.lastname
ORDER BY revenue DESC LIMIT 10;

-- Sales by product category
SELECT p.category,
       p.subcategory,
       COUNT(*)     as transactions,
       SUM(f.sales) as revenue
FROM gold.fact_sales f
         JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY revenue DESC;
```

---

## 📚 Documentation

 [Setup Guide](docs/SETUP.md) — Installation and manual runbook
 [Airflow Guide](docs/AIRFLOW.md) — Orchestrated pipeline and connections
 [Testing Guide](docs/TESTS.md) — Manual tests vs Airflow assertions
 [Data Catalog](docs/data_catalog.md) — Schemas and business rules

---

## 🔮 Future Enhancements

- Incremental loading with CDC
- Date dimension table
- Materialized views for performance
- SCD Type 2 for customer history
- BI tool integration (Power BI/Tableau)

---

## 👤 Author

**Soufiane Tazi**

**Technologies:** PostgreSQL, SQL, ETL, Data Warehousing, Apache Airflow  
**Architecture:** Medallion (Bronze-Silver-Gold), Star Schema

