/*
================================================================================
Script:     proc_load_silver.sql
Purpose:    Transform Bronze data into cleansed Silver layer
Procedure:  silver.load_silver()
================================================================================

Transforms 6 Bronze tables into Silver with:
    - Deduplication (ROW_NUMBER window functions)
    - Standardization (gender, marital status, product lines)
    - Data type conversions (INT → DATE)
    - NULL handling (COALESCE, NULLIF)
    - String parsing and trimming

Features:
    - Truncate-and-load (idempotent)
    - Logging with row counts and duration
    - Error handling with transaction rollback

Execution:
    Run AFTER: bronze.load_bronze()
    Run BEFORE: Gold layer creation

Usage:
    CALL silver.load_silver();


================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- Variables for tracking and logging
    v_start_time    TIMESTAMP; -- Individual table load start time
    v_end_time      TIMESTAMP; -- Individual table load end time
    v_batch_start   TIMESTAMP; -- Overall batch start time
    v_rows_affected INTEGER; -- Number of rows processed
    v_duration      NUMERIC; -- Duration in seconds
BEGIN
    -- Record overall batch start time
    v_batch_start := CLOCK_TIMESTAMP();

    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Starting Silver Layer Transformation';
    RAISE NOTICE 'Batch Start Time: %', v_batch_start;
    RAISE NOTICE '===============================================';

    -- ========================================================================
    -- TABLE 1: CRM_CUST_INFO - CUSTOMER MASTER DATA
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.crm_cust_info ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.crm_cust_info (cst_id,
                                      cst_key,
                                      cst_firstname,
                                      cst_lastname,
                                      cst_marital_status,
                                      cst_gndr,
                                      cst_create_date)
    SELECT cst_id,
           cst_key,
           TRIM(cst_firstname) AS cst_firstname,
           TRIM(cst_lastname)  AS cst_lastname,
           CASE
               WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
               WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
               ELSE 'n/a'
               END             AS cst_marital_status,
           CASE
               WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
               WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
               ELSE 'n/a'
               END             AS cst_gndr,
           cst_create_date
    FROM (
             -- Deduplication subquery
             SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY cst_id
                        ORDER BY cst_create_date DESC
                        ) AS flag_list
             FROM bronze.crm_cust_info
             WHERE cst_id IS NOT NULL) t
    WHERE flag_list = 1;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: Deduplication, Gender/Marital status standardization, Trimming';

    -- ========================================================================
    -- TABLE 2: CRM_PRD_INFO - PRODUCT CATALOG
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.crm_prd_info ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.crm_prd_info (prd_id,
                                     cat_id,
                                     prd_key,
                                     prd_nm,
                                     prd_cost,
                                     prd_line,
                                     prd_start_dt,
                                     prd_end_dt)
    SELECT prd_id,
           REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
           SUBSTR(prd_key, 7, LENGTH(prd_key))      AS prd_key,
           prd_nm,
           COALESCE(prd_cost, 0)                    AS prd_cost,
           CASE UPPER(TRIM(prd_line))
               WHEN 'M' THEN 'Mountain'
               WHEN 'R' THEN 'Road'
               WHEN 'S' THEN 'Other Sales'
               WHEN 'T' THEN 'Touring'
               ELSE 'n/a'
               END                                  AS prd_line,
           prd_start_dt,
           LEAD(prd_start_dt) OVER (
               PARTITION BY SUBSTR(prd_key, 7, LENGTH(prd_key))
               ORDER BY prd_start_dt
               ) - 1                                AS prd_end_dt
    FROM bronze.crm_prd_info;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: Category extraction, NULL cost handling, Product line standardization, End date calculation';

    -- ========================================================================
    -- TABLE 3: CRM_SALES_DETAILS - SALES TRANSACTIONS
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.crm_sales_details ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.crm_sales_details (sls_ord_num,
                                          sls_prd_key,
                                          sls_cust_id,
                                          sls_order_dt,
                                          sls_ship_dt,
                                          sls_due_dt,
                                          sls_sales,
                                          sls_quantity,
                                          sls_price)
    SELECT sls_ord_num,
           sls_prd_key,
           sls_cust_id,
           -- Convert INT (YYYYMMDD) to DATE
           CASE
               WHEN sls_order_dt IS NULL OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
               ELSE CAST(sls_order_dt::TEXT AS DATE)
               END AS sls_order_dt,
           CASE
               WHEN sls_ship_dt IS NULL OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
               ELSE CAST(sls_ship_dt::TEXT AS DATE)
               END AS sls_ship_dt,
           CASE
               WHEN sls_due_dt IS NULL OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
               ELSE CAST(sls_due_dt::TEXT AS DATE)
               END AS sls_due_dt,
           -- Validate and recalculate sales if needed
           CASE
               WHEN sls_sales IS NULL
                   OR sls_sales <= 0
                   OR sls_sales != ABS(sls_price) * sls_quantity
                   THEN ABS(sls_price) * sls_quantity
               ELSE sls_sales
               END AS sls_sales,
           sls_quantity,
           -- Handle invalid prices
           CASE
               WHEN sls_price IS NULL OR sls_price <= 0
                   THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
               ELSE sls_price
               END AS sls_price
    FROM bronze.crm_sales_details;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: INT to DATE conversion, Sales calculation validation, Price validation';

    -- ========================================================================
    -- FUTURE: ERP TABLES
    -- ========================================================================

    -- ========================================================================
    -- TABLE 4: ERP_CUST_AZ12 - ERP CUSTOMER DEMOGRAPHICS
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.erp_cust_az12 ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.erp_cust_az12 (cid,
                                      bdate,
                                      gen)
    SELECT CASE
               WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
               ELSE cid
               END AS cid,
           CASE
               WHEN bdate > CURRENT_DATE THEN NULL
               ELSE bdate
               END AS bdate, -- Set future birthdates to NULL
           CASE
               WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
               WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
               ELSE 'n/a'
               END AS gen
    FROM bronze.erp_cust_az12;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: Gender standardization, Birth date validation';

    -- ========================================================================
    -- TABLE 5: ERP_LOC_A101 - CUSTOMER LOCATION DATA
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.erp_loc_a101 ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.erp_loc_a101 (cid,
                                     cntry)
    SELECT REPLACE(cid, '-', '') AS cid,
           CASE
               WHEN TRIM(cntry) = 'DE' THEN 'Germany'
               WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
               WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
               ELSE TRIM(cntry)
               END               AS cntry -- Normalize and Handle missing or blank country codes
    FROM bronze.erp_loc_a101;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: Trimming, Country name standardization, NULL filtering';

    -- ========================================================================
    -- TABLE 6: ERP_PX_CAT_G1V2 - PRODUCT CATEGORY DATA
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Processing: silver.erp_px_cat_g1v2 ---';

    v_start_time := CLOCK_TIMESTAMP();

    -- Truncate target table
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Table truncated successfully';

    -- Load transformed data
    INSERT INTO silver.erp_px_cat_g1v2 (id,
                                        cat,
                                        subcat,
                                        maintenance)
    SELECT TRIM(id)            AS id,
           TRIM(UPPER(cat))    AS cat,
           TRIM(UPPER(subcat)) AS subcat,
           TRIM(maintenance)   AS maintenance
    FROM bronze.erp_px_cat_g1v2;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;
    RAISE NOTICE 'Transformations: Trimming, Category/subcategory standardization, NULL filtering';

    -- ========================================================================
    -- COMPLETION SUMMARY
    -- ========================================================================
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_batch_start))::NUMERIC, 2);

    RAISE NOTICE ' ';
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Silver Layer Transformation Completed';
    RAISE NOTICE 'Total Duration: % seconds', v_duration;
    RAISE NOTICE 'Batch End Time: %', v_end_time;
    RAISE NOTICE '===============================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE ' ';
        RAISE NOTICE '===============================================';
        RAISE NOTICE 'ERROR: Silver Layer Transformation Failed';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '===============================================';
        RAISE; -- Re-raise the exception

END;
$$;

CALL silver.load_silver()
/*
================================================================================
EXAMPLE USAGE AND TESTING:

-- 1. First, review data quality checks
\i scripts/silver/data_quality_checks.sql

-- 2. Execute the transformation procedure
CALL silver.load_silver();

-- 3. Validate results
SELECT COUNT(*) AS customer_count FROM silver.crm_cust_info;
SELECT COUNT(*) AS product_count FROM silver.crm_prd_info;
SELECT COUNT(*) AS sales_count FROM silver.crm_sales_details;

-- 4. Spot check data quality
SELECT * FROM silver.crm_cust_info LIMIT 10;
SELECT * FROM silver.crm_prd_info WHERE cat_id IS NOT NULL LIMIT 10;
SELECT * FROM silver.crm_sales_details WHERE sls_order_dt IS NOT NULL LIMIT 10;

================================================================================
END OF SCRIPT
================================================================================
*/
