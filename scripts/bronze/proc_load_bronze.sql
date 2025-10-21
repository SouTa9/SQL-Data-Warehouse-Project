/*
================================================================================
Script:     proc_load_bronze.sql
Purpose:    Load CSV files into Bronze layer tables
Procedure:  bronze.load_bronze()
================================================================================

Loads 6 CSV files into Bronze tables using COPY command:
    CRM: cust_info.csv, prd_info.csv, sales_details.csv
    ERP: CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv

Features:
    - Truncate-and-load strategy (idempotent)
    - Logging with row counts and duration
    - Error handling

⚠️  IMPORTANT: Update v_base_path_crm and v_base_path_erp variables below

Usage:
    CALL bronze.load_bronze();

================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time    TIMESTAMP;
    v_end_time      TIMESTAMP;
    v_batch_start   TIMESTAMP;
    v_rows_affected INTEGER;
    v_duration      NUMERIC;

    -- ⚠️  UPDATE THESE PATHS TO YOUR LOCAL ENVIRONMENT ⚠️
    v_base_path_crm TEXT := 'C:\Your\Path\SQL-Data-Warehouse-Project\data_sets\source_crm\';
    v_base_path_erp TEXT := 'C:\Your\Path\SQL-Data-Warehouse-Project\data_sets\source_erp\';
BEGIN
    v_batch_start := CLOCK_TIMESTAMP();

    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Starting Bronze Layer Load';
    RAISE NOTICE '=======================================';

    -- ========================================================================
    -- LOADING CRM TABLES
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Loading CRM tables ---';

    -- Load CRM Customer Information
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_cust_info';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE FORMAT('COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'cust_info.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- Load CRM Product Information
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_prd_info';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE FORMAT('COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'prd_info.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- Load CRM Sales Details
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_sales_details';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE FORMAT('COPY bronze.crm_sales_details FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'sales_details.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- ========================================================================
    -- LOADING ERP TABLES
    -- ========================================================================
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Loading ERP tables ---';

    -- Load ERP Customer Demographics
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: erp_cust_az12';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE FORMAT('COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'CUST_AZ12.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- Load ERP Location Data
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: erp_loc_a101';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE FORMAT('COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'LOC_A101.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- Load ERP Product Category Data
    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: erp_px_cat_g1v2';
    v_start_time := CLOCK_TIMESTAMP();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE FORMAT('COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'PX_CAT_G1V2.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'Loaded % rows in % seconds', v_rows_affected, v_duration;

    -- ========================================================================
    -- COMPLETION SUMMARY
    -- ========================================================================
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_batch_start))::NUMERIC, 2);

    RAISE NOTICE ' ';
    RAISE NOTICE '============================';
    RAISE NOTICE 'Bronze Layer Load Completed';
    RAISE NOTICE 'Total Duration: % seconds', v_duration;
    RAISE NOTICE '============================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Loading Failed';
        RAISE NOTICE 'Error: %', SQLERRM;
        RAISE NOTICE 'Error code: %', SQLSTATE;
        RAISE;
END;
$$;

