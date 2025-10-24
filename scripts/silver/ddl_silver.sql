/*
================================================================================
Script:     ddl_silver.sql
Purpose:    Create Silver layer tables for cleansed data
Layer:      Silver
================================================================================

Creates 6 cleansed tables with audit columns:
    CRM: crm_cust_info, crm_prd_info, crm_sales_details
    ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

Enhancements from Bronze:
    - dwh_create_date audit column
    - Prepared for data quality rules
    - Proper date types (INT → DATE conversion)

Execution:
    Run AFTER: Bronze layer populated
    Run BEFORE: proc_load_silver.sql

================================================================================
*/

-- ============================================================================
-- CRM SILVER TABLES
-- ============================================================================

-- Table: silver.crm_cust_info
-- Purpose: Cleansed customer information with deduplication
-- Transformations: Latest record per customer, standardized gender/status
DROP TABLE IF EXISTS silver.crm_cust_info CASCADE;

CREATE TABLE silver.crm_cust_info
(
    cst_id             INT,                                -- Customer unique identifier
    cst_key            VARCHAR(50),                        -- Customer business key
    cst_firstname      VARCHAR(50),                        -- Customer first name (trimmed)
    cst_lastname       VARCHAR(50),                        -- Customer last name (trimmed)
    cst_marital_status VARCHAR(50),                        -- Marital status (Married/Single/n/a)
    cst_gndr           VARCHAR(50),                        -- Gender (Male/Female/n/a)
    cst_create_date    DATE,                               -- Customer record creation date
    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);

-- Table: silver.crm_prd_info
-- Purpose: Enhanced product catalog with parsed category information
-- Transformations: Category extraction, NULL cost handling, line standardization
DROP TABLE IF EXISTS silver.crm_prd_info CASCADE;
CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,                                -- Product unique identifier
    prd_key         VARCHAR(50),                        -- Product business key
    prd_nm          VARCHAR(50),                        -- Product name
    prd_cost        INT,                                -- Product cost (handled for NULLs)
    cat_id          VARCHAR(50),                        -- Category ID (extracted from prd_key)
    prd_line        VARCHAR(50),                        -- Product line (Mountain/Road/Touring/etc.)
    prd_start_dt    DATE,                               -- Product availability start date
    prd_end_dt      DATE,                               -- Product availability end date (calculated)
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);

-- Table: silver.crm_sales_details
-- Purpose: Cleansed sales transactions with correct data types
-- Transformations: Date conversion, numeric validation
DROP TABLE IF EXISTS silver.crm_sales_details CASCADE;
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     VARCHAR(50),                        -- Sales order number (primary identifier)
    sls_prd_key     VARCHAR(50),                        -- Product key
    sls_cust_id     INT,                                -- Customer ID
    sls_order_dt    DATE,                               -- Order date (converted from YYYYMMDD INT)
    sls_ship_dt     DATE,                               -- Ship date (converted from YYYYMMDD INT)
    sls_due_dt      DATE,                               -- Due date (converted from YYYYMMDD INT)
    sls_sales       INT,                                -- Total sales amount (validated)
    sls_quantity    INT,                                -- Quantity ordered
    sls_price       INT,                                -- Unit price
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);


-- ============================================================================
-- ERP SILVER TABLES
-- ============================================================================

-- Table: silver.erp_cust_az12
-- Purpose: Standardized customer demographic data
-- Transformations: Gender normalization, prefixed ID
DROP TABLE IF EXISTS silver.erp_cust_az12 CASCADE;
CREATE TABLE silver.erp_cust_az12
(
    cid             VARCHAR(50),                        -- Customer ID with prefix
    bdate           DATE,                               -- Birth date
    gen             VARCHAR(50),                        -- Gender (standardized)
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);

-- Table: silver.erp_loc_a101
-- Purpose: Standardized customer location/country information
-- Transformations: Country name normalization
DROP TABLE IF EXISTS silver.erp_loc_a101 CASCADE;
CREATE TABLE silver.erp_loc_a101
(
    cid             VARCHAR(50),                        -- Customer ID (matches CRM keys)
    cntry           VARCHAR(50),                        -- Country name (standardized)
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);

-- Table: silver.erp_px_cat_g1v2
-- Purpose: Normalized product category hierarchy
-- Transformations: Category/subcategory standardization
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2 CASCADE;

CREATE TABLE silver.erp_px_cat_g1v2
(
    id              VARCHAR(50),                        -- Product ID
    cat             VARCHAR(50),                        -- Product category
    subcat          VARCHAR(50),                        -- Product subcategory
    maintenance     VARCHAR(50),                        -- Maintenance cost/flag
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Audit: When loaded into warehouse
);

/*
================================================================================
END OF SCRIPT
================================================================================
*/