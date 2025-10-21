/*
================================================================================
Script:     data_quality_checks_silver.sql
Purpose:    Data quality validation for Silver layer ETL
Layer:      Silver
================================================================================

Pre-transformation checks for Bronze data:
    - Duplicate detection
    - NULL value analysis
    - Data pattern validation
    - Categorical value distribution

Usage:
    1. Run after Bronze layer is loaded
    2. Review results to understand data quality
    3. Execute silver.load_silver() when satisfied
    4. Re-run checks to validate Silver data

================================================================================
*/

-- ============================================================================
-- TABLE 1: CRM_CUST_INFO - CUSTOMER INFORMATION CHECKS
-- ============================================================================

RAISE NOTICE '============================================';
RAISE NOTICE 'DATA QUALITY CHECKS: CRM_CUST_INFO';
RAISE NOTICE '============================================';

-- Check 1.1: Find duplicate customer IDs and NULL values
-- Purpose: Identify data quality issues that need deduplication logic
-- Expected: Some duplicates found (handled by ROW_NUMBER in transformation)
RAISE NOTICE 'Check 1.1: Duplicate and NULL customer IDs';
SELECT cst_id, COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
    OR cst_id IS NULL;

-- Check 1.2: Examine duplicate records in detail
-- Purpose: Understand which record to keep (latest by cst_create_date)
RAISE NOTICE 'Check 1.2: Detailed view of duplicate records';
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1
)
ORDER BY cst_id, cst_create_date DESC;

-- Check 1.3: Preview deduplication logic using ROW_NUMBER window function
-- Purpose: Validate that ROW_NUMBER correctly identifies latest record
-- Logic: PARTITION BY cst_id (group by customer) ORDER BY cst_create_date DESC (latest first)
RAISE NOTICE 'Check 1.3: Deduplication preview (flag_last = 1 will be kept)';
SELECT cst_id,
       cst_key,
       cst_create_date,
       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1
)
ORDER BY cst_id, flag_last;

-- Check 1.4: Find records with unwanted leading/trailing spaces in marital status
-- Purpose: Identify if TRIM is needed (data cleansing)
RAISE NOTICE 'Check 1.4: Records with spaces in marital status';
SELECT cst_id,
       cst_marital_status,
       LENGTH(cst_marital_status) AS original_length,
       LENGTH(TRIM(cst_marital_status)) AS trimmed_length
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- Check 1.5: Find records with unwanted spaces in customer key
RAISE NOTICE 'Check 1.5: Records with spaces in customer key';
SELECT cst_id,
       cst_key,
       LENGTH(cst_key) AS original_length,
       LENGTH(TRIM(cst_key)) AS trimmed_length
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Check 1.6: Examine distinct values for categorical fields
RAISE NOTICE 'Check 1.6: Distinct marital status values';
SELECT DISTINCT cst_marital_status, COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_marital_status
ORDER BY count DESC;

RAISE NOTICE 'Check 1.7: Distinct gender values';
SELECT DISTINCT cst_gndr, COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_gndr
ORDER BY count DESC;


-- ============================================================================
-- TABLE 2: CRM_PRD_INFO - PRODUCT INFORMATION CHECKS
-- ============================================================================

RAISE NOTICE ' ';
RAISE NOTICE '============================================';
RAISE NOTICE 'DATA QUALITY CHECKS: CRM_PRD_INFO';
RAISE NOTICE '============================================';

-- Check 2.1: Preview raw product data structure
RAISE NOTICE 'Check 2.1: Sample product records';
SELECT *
FROM bronze.crm_prd_info
LIMIT 10;

-- Check 2.2: Find duplicate product IDs
-- Purpose: Ensure product_id is a valid primary key
-- Expected: May have duplicates if tracking product versions over time
RAISE NOTICE 'Check 2.2: Duplicate product IDs';
SELECT prd_id, COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Check 2.3: Check for NULL or negative costs
-- Purpose: Identify missing or invalid cost data
RAISE NOTICE 'Check 2.3: Invalid product costs (NULL or negative)';
SELECT COUNT(*) AS invalid_cost_count,
       SUM(CASE WHEN prd_cost IS NULL THEN 1 ELSE 0 END) AS null_costs,
       SUM(CASE WHEN prd_cost < 0 THEN 1 ELSE 0 END) AS negative_costs
FROM bronze.crm_prd_info;

-- Check 2.4: Examine distinct product line values
-- Purpose: Understand what standardization is needed
-- Expected: Single letter codes (M, R, S, T) that need expansion
RAISE NOTICE 'Check 2.4: Product line distribution';
SELECT DISTINCT prd_line,
       LENGTH(prd_line) AS code_length,
       COUNT(*) AS count
FROM bronze.crm_prd_info
GROUP BY prd_line
ORDER BY count DESC;

-- Check 2.5: Validate date logic - end date should be after start date
-- Purpose: Find data integrity issues
RAISE NOTICE 'Check 2.5: Invalid date ranges (end < start)';
SELECT prd_id, prd_key, prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check 2.6: Analyze product key structure for parsing
RAISE NOTICE 'Check 2.6: Product key structure analysis';
SELECT SUBSTR(prd_key, 1, 5) AS category_part,
       SUBSTR(prd_key, 7, LENGTH(prd_key)) AS product_part,
       COUNT(*) AS count
FROM bronze.crm_prd_info
GROUP BY SUBSTR(prd_key, 1, 5), SUBSTR(prd_key, 7, LENGTH(prd_key))
ORDER BY count DESC
LIMIT 10;


-- ============================================================================
-- TABLE 3: CRM_SALES_DETAILS - SALES TRANSACTIONS CHECKS
-- ============================================================================

RAISE NOTICE ' ';
RAISE NOTICE '============================================';
RAISE NOTICE 'DATA QUALITY CHECKS: CRM_SALES_DETAILS';
RAISE NOTICE '============================================';

-- Check 3.1: Preview raw sales data
RAISE NOTICE 'Check 3.1: Sample sales records';
SELECT *
FROM bronze.crm_sales_details
LIMIT 10;

-- Check 3.2: Find duplicate order numbers (primary key validation)
-- Expected: No duplicates - each order number should be unique
RAISE NOTICE 'Check 3.2: Duplicate order numbers';
SELECT sls_ord_num, COUNT(*) AS duplicate_count
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1;

-- Check 3.3: Check for unwanted spaces in order numbers
RAISE NOTICE 'Check 3.3: Order numbers with spaces';
SELECT COUNT(*) AS records_with_spaces
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check 3.4: Validate date fields (stored as integers in YYYYMMDD format)
-- Purpose: Find invalid dates before conversion
-- Valid range: 19000101 to 20500101, exactly 8 digits
RAISE NOTICE 'Check 3.4: Invalid order dates';
SELECT sls_ord_num,
       sls_order_dt,
       LENGTH(sls_order_dt::TEXT) AS date_length
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_order_dt <= 0
   OR LENGTH(sls_order_dt::TEXT) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101
LIMIT 20;

-- Check 3.5: Validate date logic - order date should be before ship/due dates
RAISE NOTICE 'Check 3.5: Illogical date sequences';
SELECT COUNT(*) AS illogical_dates,
       SUM(CASE WHEN sls_order_dt > sls_ship_dt THEN 1 ELSE 0 END) AS order_after_ship,
       SUM(CASE WHEN sls_order_dt > sls_due_dt THEN 1 ELSE 0 END) AS order_after_due
FROM bronze.crm_sales_details;

-- Check 3.6: Validate business rule - sales amount should equal price × quantity
-- Purpose: Find calculation errors or data entry mistakes
RAISE NOTICE 'Check 3.6: Sales calculation mismatches';
SELECT COUNT(*) AS total_mismatches,
       SUM(CASE WHEN sls_sales IS NULL THEN 1 ELSE 0 END) AS null_sales,
       SUM(CASE WHEN sls_price IS NULL THEN 1 ELSE 0 END) AS null_price,
       SUM(CASE WHEN sls_quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
       SUM(CASE WHEN sls_sales != sls_price * sls_quantity THEN 1 ELSE 0 END) AS calculation_errors
FROM bronze.crm_sales_details;

-- Check 3.7: Detailed view of calculation mismatches
RAISE NOTICE 'Check 3.7: Sample calculation mismatches';
SELECT sls_ord_num,
       sls_sales,
       sls_price,
       sls_quantity,
       (sls_price * sls_quantity) AS calculated_sales,
       (sls_sales - (sls_price * sls_quantity)) AS difference
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
   AND sls_sales IS NOT NULL
   AND sls_price IS NOT NULL
   AND sls_quantity IS NOT NULL
LIMIT 10;


-- ============================================================================
-- TABLE 4: ERP_CUST_AZ12 - ERP CUSTOMER DEMOGRAPHICS CHECKS
-- ============================================================================

RAISE NOTICE ' ';
RAISE NOTICE '============================================';
RAISE NOTICE 'DATA QUALITY CHECKS: ERP_CUST_AZ12';
RAISE NOTICE '============================================';

-- Check 4.1: Preview raw ERP customer data
RAISE NOTICE 'Check 4.1: Sample ERP customer records';
SELECT *
FROM bronze.erp_cust_az12
LIMIT 10;

-- Check 4.2: Find duplicate customer IDs
RAISE NOTICE 'Check 4.2: Duplicate ERP customer IDs';
SELECT cid, COUNT(*) AS duplicate_count
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- Check 4.3: Validate customer ID format and check CRM match
-- Purpose: ERP uses "AW_00000001" format, CRM uses "AW00000001"
RAISE NOTICE 'Check 4.3: Customer ID format validation';
SELECT DISTINCT SUBSTR(cid, 1, 3) AS prefix,
       COUNT(*) AS count
FROM bronze.erp_cust_az12
GROUP BY SUBSTR(cid, 1, 3);

-- Check 4.4: Referential integrity check - Find ERP customers NOT in CRM
-- Purpose: Understand data completeness between systems
RAISE NOTICE 'Check 4.4: ERP customers missing in CRM';
SELECT COUNT(*) AS erp_only_customers
FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- Check 4.5: Validate birth dates
RAISE NOTICE 'Check 4.5: Invalid birth dates';
SELECT COUNT(*) AS invalid_dates,
       SUM(CASE WHEN bdate IS NULL THEN 1 ELSE 0 END) AS null_dates,
       SUM(CASE WHEN bdate > CURRENT_DATE THEN 1 ELSE 0 END) AS future_dates,
       SUM(CASE WHEN bdate < '1900-01-01' THEN 1 ELSE 0 END) AS too_old_dates
FROM bronze.erp_cust_az12;

-- Check 4.6: Gender value distribution
RAISE NOTICE 'Check 4.6: Gender distribution';
SELECT gen, COUNT(*) AS count
FROM bronze.erp_cust_az12
GROUP BY gen
ORDER BY count DESC;


/*
================================================================================
SUMMARY AND NEXT STEPS:

After running all these checks:
1. Review any data quality issues found
2. Document findings and decide on handling strategies
3. Update transformation logic in proc_load_silver.sql if needed
4. Run the transformation procedure
5. Validate Silver layer data using similar checks

Common Data Quality Issues to Watch For:
- Duplicates (require deduplication strategy)
- NULL values (decide: filter out, impute, or keep with default)
- Invalid formats (require parsing or standardization)
- Referential integrity violations (orphan records)
- Business rule violations (e.g., sales ≠ price × quantity)

================================================================================
END OF SCRIPT
================================================================================
*/

