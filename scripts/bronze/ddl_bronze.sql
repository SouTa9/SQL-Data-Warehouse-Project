/*
================================================================================
Script:     ddl_bronze.sql
Purpose:    Create Bronze layer tables for raw data ingestion
Layer:      Bronze
================================================================================

Creates 6 tables mirroring source systems:
    CRM: crm_cust_info, crm_prd_info, crm_sales_details
    ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

Characteristics:
    - No transformations or constraints
    - All columns nullable
    - Mirrors CSV structure exactly
    - Landing zone for raw data

Execution:
    Run AFTER: 00_init_database.sql
    Run BEFORE: proc_load_bronze.sql


================================================================================
*/

-- ============================================================================
-- CRM SOURCE TABLES
-- ============================================================================

-- Table: bronze.crm_cust_info
-- Purpose: Raw customer information from CRM system
-- Source: data_sets/source_crm/cust_info.csv
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info
(
    cst_id             INT,         -- Customer unique identifier
    cst_key            VARCHAR(50), -- Customer business key (e.g., AW00000001)
    cst_firstname      VARCHAR(50), -- Customer first name
    cst_lastname       VARCHAR(50), -- Customer last name
    cst_marital_status VARCHAR(50), -- Marital status (raw: M, S, etc.)
    cst_gndr           VARCHAR(50), -- Gender (raw: M, F, etc.)
    cst_create_date    DATE         -- Customer record creation date
);

-- Table: bronze.crm_prd_info
-- CRM Source Tables
-- Source: data_sets/source_crm/
(
    prd_cost     INT,         -- Product cost (can be NULL)
    prd_line     VARCHAR(50), -- Product line code (M, R, S, T)
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE
-- Note: Date fields stored as INT in YYYYMMDD format
DROP TABLE IF EXISTS bronze.crm_sales_details;
    sls_ord_num  VARCHAR(50), -- Sales order number (primary identifier)
    sls_prd_key  VARCHAR(50), -- Product key (FK to crm_prd_info)
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

-- Table: bronze.erp_cust_az12
-- Source: data_sets/source_erp/CUST_AZ12.csv
-- Note: Cryptic naming convention from legacy ERP system
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,  -- YYYYMMDD format
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
-- Source: data_sets/source_erp/LOC_A101.csv
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101
-- ERP Source Tables
-- Source: data_sets/source_erp/
);
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12
-- Purpose: Product category and maintenance data from ERP (Group G1V2)
-- Source: data_sets/source_erp/PX_CAT_G1V2.csv
CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2
(
    ID          VARCHAR(50), -- Product ID
    CAT         VARCHAR(50), -- Product category
    SUBCAT      VARCHAR(50), -- Product subcategory
    MAINTENANCE VARCHAR(50)  -- Maintenance cost or flag
);

/*
================================================================================
END OF SCRIPT
================================================================================
*/
