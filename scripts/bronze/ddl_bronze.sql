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
DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE;
CREATE TABLE bronze.crm_cust_info
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
-- Purpose: Raw product information from CRM system
-- Source: data_sets/source_crm/prd_info.csv
DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE;
CREATE TABLE bronze.crm_prd_info
(
    prd_id       INT,         -- Product unique identifier
    prd_key      VARCHAR(50), -- Product business key
    prd_nm       VARCHAR(50), -- Product name
    prd_cost     INT,         -- Product cost
    prd_line     VARCHAR(50), -- Product line
    prd_start_dt DATE,        -- Product start date
    prd_end_dt   DATE         -- Product end date
);

-- Table: bronze.crm_sales_details
-- Purpose: Raw sales transaction data from CRM system
-- Source: data_sets/source_crm/sales_details.csv
-- Note: Date fields stored as INT in YYYYMMDD format
DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;
CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num  VARCHAR(50), -- Sales order number (primary identifier)
    sls_prd_key  VARCHAR(50), -- Product key (FK to crm_prd_info)
    sls_cust_id  INT,         -- Customer ID (FK to crm_cust_info)
    sls_order_dt INT,         -- Sales order date (YYYYMMDD)
    sls_ship_dt  INT,         -- Sales ship date (YYYYMMDD)
    sls_due_dt   INT,         -- Sales due date (YYYYMMDD)
    sls_sales    INT,         -- Sales amount
    sls_quantity INT,         -- Order quantity
    sls_price    INT          -- Unit price
);


-- ============================================================================
-- ERP SOURCE TABLES
-- ============================================================================

-- Table: bronze.erp_cust_az12
-- Purpose: Customer demographic data from ERP (System AZ12)
-- Source: data_sets/source_erp/CUST_AZ12.csv
-- Note: Cryptic naming convention from legacy ERP system
DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
CREATE TABLE bronze.erp_cust_az12
(
    cid   VARCHAR(50), -- Customer ID (may contain prefixes like NAS/AW)
    bdate DATE,        -- Birth date
    gen   VARCHAR(10)  -- Gender
);

-- Table: bronze.erp_loc_a101
-- Purpose: Customer location data from ERP (System A101)
-- Source: data_sets/source_erp/LOC_A101.csv
DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
CREATE TABLE bronze.erp_loc_a101
(
    cid   VARCHAR(50), -- Customer ID (text to match CRM keys)
    cntry VARCHAR(50)  -- Country
);

-- Table: bronze.erp_px_cat_g1v2
-- Purpose: Product category and maintenance data from ERP (Group G1V2)
-- Source: data_sets/source_erp/PX_CAT_G1V2.csv
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;
CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          VARCHAR(50), -- Product ID
    cat         VARCHAR(50), -- Product category
    subcat      VARCHAR(50), -- Product subcategory
    maintenance VARCHAR(50)  -- Maintenance cost or flag
);

/*
================================================================================
END OF SCRIPT
================================================================================
*/
