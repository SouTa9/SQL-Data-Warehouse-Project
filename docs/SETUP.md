# Setup Guide

Complete step-by-step instructions to set up and run the SQL Data Warehouse project.

---

## Prerequisites

### Required Software

- **PostgreSQL** 12 or higher ([Download](https://www.postgresql.org/download/))
- **Database Client** (choose one):
    - pgAdmin (GUI) - Recommended for beginners
    - DataGrip (Commercial IDE)
    - DBeaver (Open-source)
    - psql (Command-line)

### Required Access

- PostgreSQL superuser or database creation privileges
- File system read access for CSV files

---

## Installation Steps

### 1. Clone or Download Project

```bash
# Download project files to your local machine
cd C:\Your\Workspace\
```

Ensure you have all files:

```
SQL-Data-Warehouse-Project/
├── data_sets/
├── scripts/
├── tests/
└── docs/
```

---

### 2. Update File Paths

**IMPORTANT:** Edit the file paths in the Bronze loading procedure.

Open: `scripts/bronze/proc_load_bronze.sql`

Find lines 62-63 and update to your local paths:

```sql
-- BEFORE (example):
v_base_path_crm TEXT := 'C:\Users\vlado\Desktop\SQL-project-last\...';
v_base_path_erp TEXT := 'C:\Users\vlado\Desktop\SQL-project-last\...';

-- AFTER (your actual path):
v_base_path_crm TEXT := 'C:\Your\Path\SQL-Data-Warehouse-Project\data_sets\source_crm\';
v_base_path_erp TEXT := 'C:\Your\Path\SQL-Data-Warehouse-Project\data_sets\source_erp\';
```

**Windows Users:** Use double backslashes `\\` or forward slashes `/`

---

## Execution

### Option A: Using psql (Command-Line)

```bash
# 1. Connect to PostgreSQL
psql -U postgres

# 2. Initialize database and schemas
\i C:/Your/Path/SQL-Data-Warehouse-Project/scripts/00_init_database.sql

# 3. Connect to the new database
\c data_warehouse

# 4. Create and load Bronze layer
\i C:/Your/Path/SQL-Data-Warehouse-Project/scripts/bronze/ddl_bronze.sql
CALL bronze.load_bronze();

# 5. Create and load Silver layer
\i C:/Your/Path/SQL-Data-Warehouse-Project/scripts/silver/ddl_silver.sql
CALL silver.load_silver();

# 6. Create Gold layer
\i C:/Your/Path/SQL-Data-Warehouse-Project/scripts/gold/ddl_gold.sql

# 7. Validate data quality
\i C:/Your/Path/SQL-Data-Warehouse-Project/tests/data_quality_checks_gold.sql
```

---

### Option B: Using pgAdmin (GUI)

**Step 1: Initialize Database**

1. Right-click on PostgreSQL server → Query Tool
2. Open file: `scripts/00_init_database.sql`
3. Click Execute (F5)
4. Refresh server tree to see `data_warehouse` database

**Step 2: Bronze Layer**

1. Right-click `data_warehouse` → Query Tool
2. Open and execute: `scripts/bronze/ddl_bronze.sql`
3. Open and execute: `scripts/bronze/proc_load_bronze.sql`
4. Run: `CALL bronze.load_bronze();`
5. Check logs for success message

**Step 3: Silver Layer**

1. Open and execute: `scripts/silver/ddl_silver.sql`
2. Open and execute: `scripts/silver/proc_load_silver.sql`
3. Run: `CALL silver.load_silver();`
4. Check logs for row counts

**Step 4: Gold Layer**

1. Open and execute: `scripts/gold/ddl_gold.sql`
2. Verify views created:
   ```sql
   SELECT table_name FROM information_schema.views
   WHERE table_schema = 'gold';
   ```

**Step 5: Data Quality Checks**

1. Open and execute: `tests/data_quality_checks_gold.sql`
2. Review all test results
3. Ensure no unexpected failures

---

## Verification Queries

After setup, verify each layer:

### Check Bronze Layer

```sql
SELECT 'crm_cust_info' as table_name, COUNT(*) as records FROM bronze.crm_cust_info
UNION ALL
SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details;
```

**Expected:** ~700, ~300, ~60,000 records

### Check Silver Layer

```sql
SELECT 'crm_cust_info' as table_name, COUNT(*) as records FROM silver.crm_cust_info
UNION ALL
SELECT 'crm_prd_info', COUNT(*) FROM silver.crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM silver.crm_sales_details;
```

**Expected:** Similar counts to Bronze (may differ after cleansing)

### Check Gold Layer

```sql
-- Verify dimensions
SELECT COUNT(*) as customer_count FROM gold.dim_customer;
SELECT COUNT(*) as product_count FROM gold.dim_products;
SELECT COUNT(*) as sales_count FROM gold.fact_sales;
```

**Expected:** ~700 customers, ~200 products, ~60,000 sales

### Sample Analytics Query

```sql
-- Top 5 customers by revenue
SELECT c.firstname, c.lastname, 
       SUM(f.sales) as total_revenue,
       COUNT(*) as order_count
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.firstname, c.lastname
ORDER BY total_revenue DESC
LIMIT 5;
```

---

## Troubleshooting

### Issue: "relation does not exist"

**Solution:** Ensure you're connected to `data_warehouse` database

```sql
SELECT current_database();  -- Should show: data_warehouse
```

### Issue: "could not open file"

**Solution:** Check file paths in `proc_load_bronze.sql`

- Ensure paths are absolute
- Use forward slashes or double backslashes on Windows
- Verify CSV files exist at specified locations

### Issue: "permission denied"

**Solution:** Grant PostgreSQL read access to CSV directory

```bash
# Windows: Check folder permissions
# Linux/Mac: chmod +r data_sets/
```

### Issue: Stored procedure fails

**Solution:** Check logs for specific error

```sql
-- Re-run with verbose logging
CALL bronze.load_bronze();
-- Review RAISE NOTICE messages in output
```

### Issue: Different row counts than expected

**Solution:** This is normal - data may vary by source files

- Verify Bronze loads successfully
- Check Silver transformations remove duplicates
- Gold may have fewer products (active only filter)

---

## Re-running the Pipeline

The ETL is **idempotent** (can be run multiple times safely):

```sql
-- To reload everything from scratch:
CALL bronze.load_bronze();  -- Truncates and reloads
CALL silver.load_silver();  -- Truncates and reloads

-- Gold views are automatically updated (they query Silver)
SELECT * FROM gold.dim_customer LIMIT 5;
```

---

## Next Steps

After successful setup:

1. **Explore the data:**
    - Review [DATA_CATALOG.md](DATA_CATALOG.md)
    - Run sample queries from [README.md](../README.md)

2. **Understand transformations:**
    - Read SQL comments in `proc_load_silver.sql`
    - Review [ARCHITECTURE.md](ARCHITECTURE.md)

3. **Connect BI tools:**
    - Power BI, Tableau, Looker, etc.
    - Connect to `data_warehouse` database
    - Query Gold layer views

4. **Experiment:**
    - Modify transformations
    - Add new dimensions
    - Create custom aggregations

---

## Clean Up

To remove the database:

```sql
-- Disconnect from data_warehouse first
\c postgres

-- Drop the database
DROP DATABASE IF EXISTS data_warehouse;
```

---

**Need help?** Review error messages in the console output. Most issues are related to file paths or permissions.

# Architecture Documentation

## System Overview

This data warehouse implements a **Medallion Architecture** pattern with three progressive layers of data refinement,
designed to support business intelligence and analytics workloads.

---

## Architecture Pattern: Medallion (Bronze-Silver-Gold)

### Why This Architecture?

**Progressive Data Quality:** Each layer improves data quality and usability

- Bronze: Raw, unprocessed data (single source of truth)
- Silver: Cleansed, validated data (trusted analytics foundation)
- Gold: Business-optimized models (BI-ready)

**Benefits:**

- Clear separation of concerns
- Traceable data lineage
- Flexible reprocessing
- Incremental complexity
- Support for multiple use cases

---

## Layer Details

### Bronze Layer - Raw Data Landing

**Purpose:** Preserve source data exactly as received

**Characteristics:**

- No transformations or validations
- Full truncate-and-load strategy
- All columns nullable
- Mirrors source system structure

**Design Decisions:**

- COPY command for efficient bulk loading
- No primary keys (enforced in Silver)
- Preserves data for audit and reprocessing

---

### Silver Layer - Trusted Analytics Foundation

**Purpose:** Provide clean, validated data for downstream consumption

**Key Transformations:**

1. **Data Quality**
    - Deduplication (ROW_NUMBER window function)
    - NULL handling (COALESCE, NULLIF)
    - Data type conversions (INT → DATE)

2. **Standardization**
    - Gender: M→Male, F→Female
    - Marital status: M→Married, S→Single
    - Product lines: R→Road, M→Mountain, etc.

3. **Parsing & Enrichment**
    - Category extraction from product keys
    - SCD Type 2 using LEAD() window function
    - Whitespace trimming

**Design Decisions:**

- Stored procedures for repeatable ETL
- Audit columns (dwh_create_date) for lineage
- Idempotent operations (can re-run safely)

---

### Gold Layer - Business Intelligence

**Purpose:** Dimensional model optimized for analytics and reporting

**Schema Design:** Star Schema

- 2 Dimensions (customer, products)
- 1 Fact (sales transactions)
- Surrogate keys for performance

**Key Features:**

- Multi-source integration (CRM + ERP)
- Business-friendly naming
- Implemented as views (real-time)
- Pre-joined for easy querying

**Design Decisions:**

- Views vs. tables: Flexibility for small datasets
- Surrogate keys: ROW_NUMBER() for joins
- Active products only (business requirement)
- Gender resolution logic (CRM priority)

---

## Data Integration Strategy

### Customer Integration

```
CRM Customer Info (primary)
    ├─ Core attributes: name, ID, marital status, gender
    ├─ JOIN → ERP Demographics: birthdate
    └─ JOIN → ERP Locations: country
    
Result: Unified customer dimension (~700 customers)
```

**Resolution Logic:**

- Gender: CRM takes precedence over ERP (if not 'n/a')
- Left joins preserve all CRM customers
- Missing ERP data handled gracefully

### Product Integration

```
CRM Product Info (primary)
    └─ JOIN → ERP Categories: category hierarchy
    
Filter: WHERE prd_end_dt IS NULL (active only)

Result: Active product catalog (~200 products)
```

### Sales Processing

```
CRM Sales Details
    ├─ Validate: dates, calculations
    ├─ Lookup → dim_customer: customer_key
    └─ Lookup → dim_products: product_key
    
Result: Fact table with dimension keys (~60K transactions)
```

---

## ETL Pipeline

### Execution Flow

```
1. Initialize Database
   └─ Create database and schemas

2. Bronze Layer
   ├─ Create tables (DDL)
   └─ Load CSV files (stored procedure)

3. Silver Layer
   ├─ Create tables (DDL)
   ├─ Run quality checks (optional)
   └─ Transform data (stored procedure)

4. Gold Layer
   ├─ Create views (DDL)
   └─ Run quality validation (35+ tests)

5. Ready for BI Tools
```

### Error Handling

- **Bronze:** Accepts all data (no failures)
- **Silver:** Transaction rollback on errors
- **Gold:** Views fail gracefully if Silver incomplete

### Performance Considerations

- **Loading:** COPY command for bulk inserts
- **Transformations:** Window functions optimized for single pass
- **Queries:** Surrogate keys enable fast joins
- **Scalability:** Views can be materialized for large datasets

---

## Technology Stack

**Database:** PostgreSQL 12+

- Reason: Open-source, powerful window functions, strong ETL support

**SQL Dialect:** plpgsql

- Reason: Stored procedures, error handling, logging

**Loading Method:** COPY command

- Reason: Fastest bulk loading for CSV files

**Gold Implementation:** Views

- Reason: Real-time data, flexibility, easy maintenance

---

## Design Decisions & Trade-offs

### Why Views Instead of Tables in Gold?

**Pros:**

- Always up-to-date with Silver layer
- No additional storage
- Easy to modify logic

**Cons:**

- Query performance overhead for large datasets
- Consider materialized views for production scale

### Why Full Load Instead of Incremental?

**Current Approach:** Truncate and reload

- Simpler implementation
- No state management
- Suitable for datasets <1M rows

**Future Enhancement:** CDC (Change Data Capture)

- Track only changes
- Better performance at scale
- Requires more complex logic

### Why Surrogate Keys?

**ROW_NUMBER() vs. SERIAL:**

- ROW_NUMBER: Deterministic, reproducible
- SERIAL: Requires table persistence
- Choice: ROW_NUMBER for view-based Gold layer

---

## Data Quality Framework

### Testing Pyramid

```
           /\
          /35\      Gold Layer (35 tests)
         /____\     - Business logic validation
        /      \    - Cross-dimensional checks
       /  20+   \   Silver Layer (20+ tests)
      /__________\  - Data cleansing validation
     /            \ Bronze Layer (minimal)
    /______________\- Load verification only
```

### Validation Types

1. **Completeness:** No missing critical values
2. **Accuracy:** Data matches business rules
3. **Consistency:** Uniform across sources
4. **Validity:** Proper formats and ranges
5. **Integrity:** Referential relationships maintained
6. **Uniqueness:** No unexpected duplicates

---

## Scalability Path

### Current State (Prototype)

- Dataset: ~60K transactions
- Loading: Full truncate-reload
- Gold: Views
- Execution: Manual

### Production Scale (Future)

- Dataset: Millions of transactions
- Loading: Incremental with CDC
- Gold: Materialized views with refresh
- Execution: Scheduled jobs (cron/Airflow)
- Monitoring: Data quality dashboard
- Testing: Automated CI/CD pipeline

---

## Security Considerations

**Current Implementation:**

- Schema-based separation
- Database-level access control

**Production Recommendations:**

- Row-level security for sensitive data
- Audit logging for data access
- Encryption at rest and in transit
- Role-based access control (RBAC)

---

## Naming Conventions

**Schemas:** `bronze`, `silver`, `gold`

**Tables:**

- Bronze/Silver: `<source>_<entity>` (e.g., `crm_cust_info`)
- Gold: `<type>_<entity>` (e.g., `dim_customer`)

**Columns:**

- snake_case throughout
- Prefix pattern: `cst_` (customer), `prd_` (product), `sls_` (sales)
- Keys: `<entity>_key` (surrogate), `<entity>_id` (natural)

**Procedures:** `load_<layer>()` (e.g., `bronze.load_bronze()`)

---

*This architecture balances simplicity with best practices, making it suitable for portfolio demonstration while
maintaining production-ready principles.*

