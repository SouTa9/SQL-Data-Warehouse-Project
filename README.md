# SQL Data Warehouse Project

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)](https://www.postgresql.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)](https://www.databricks.com/glossary/medallion-architecture)

## 📊 Overview

End-to-end data warehouse implementing the **Medallion Architecture** (Bronze-Silver-Gold) pattern. Integrates CRM and
ERP data sources into a unified analytics platform with dimensional modeling for business intelligence.

**Key Features:**

- Full ETL pipeline with automated quality checks
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
├── data_sets/
│   ├── source_crm/          # CRM: customers, products, sales (~60K records)
│   └── source_erp/          # ERP: demographics, locations, categories
├── scripts/
│   ├── 00_init_database.sql # Database initialization
│   ├── bronze/              # Raw data ingestion (DDL + procedures)
│   ├── silver/              # Data transformation (DDL + procedures)
│   └── gold/                # Star schema views (2 dims, 1 fact)
├── tests/                   # Data quality validation (35+ checks)
└── docs/                    # Data catalog and diagrams
```

---

## 🗄️ Database Schema

### Gold Layer - Star Schema (Ready for BI)

| Object         | Type      | Description                        | Records |
|----------------|-----------|------------------------------------|---------|
| `dim_customer` | Dimension | Customer master (CRM + ERP merged) | ~700    |
| `dim_products` | Dimension | Active products with categories    | ~200    |
| `fact_sales`   | Fact      | Sales transactions with metrics    | ~60K    |

**Bronze & Silver Layers:** See [docs/DATA_CATALOG.md](docs/DATA_CATALOG.md) for detailed schemas.

---

## 🚀 Quick Start

### Prerequisites

- PostgreSQL 12+
- Database client (pgAdmin, DataGrip, DBeaver, or psql)
- Database creation privileges

### Setup & Execution

```bash
# 1. Initialize database and schemas
psql -U postgres -f scripts/00_init_database.sql

# 2. Connect to the warehouse
psql -U postgres -d data_warehouse

# 3. Create and load Bronze layer (update file paths first)
\i scripts/bronze/ddl_bronze.sql
CALL bronze.load_bronze();

# 4. Create and load Silver layer
\i scripts/silver/ddl_silver.sql
CALL silver.load_silver();

# 5. Create Gold layer views
\i scripts/gold/ddl_gold.sql

# 6. Run data quality checks
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

### Data Quality (35+ Tests)

- Completeness, accuracy, consistency validation
- Referential integrity checks
- Business rule enforcement
- NULL and duplicate detection

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
         JOIN gold.dim_customer c ON f.customer_key = c.customer_key
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

- **[Data Catalog](docs/DATA_CATALOG.md)** - Complete data dictionary
- **[Architecture](docs/ARCHITECTURE.md)** - System design decisions
- **[Setup Guide](docs/SETUP.md)** - Installation instructions

---

## 🔮 Future Enhancements

- Incremental loading with CDC
- Date dimension table
- Materialized views for performance
- SCD Type 2 for customer history
- BI tool integration (Power BI/Tableau)

---

## 👤 Author

**Portfolio Project** demonstrating data engineering best practices

**Technologies:** PostgreSQL, SQL, ETL, Data Warehousing  
**Architecture:** Medallion (Bronze-Silver-Gold), Star Schema

