CREATE OR REPLACE PROCEDURE bronze.load_bronze()
    LANGUAGE plpgsql
AS
$$
DECLARE
    -- Variables
    v_start_time    TIMESTAMP;
    v_end_time      TIMESTAMP;
    v_batch_start   TIMESTAMP;
    v_rows_affected INTEGER;
    v_duration      NUMERIC;
    v_base_path_crm TEXT := 'C:\Users\vlado\Desktop\SQL-project4\SQL-Data-Warehouse-Project\data_sets\source_crm\';
    v_base_path_erp TEXT := 'C:\Users\vlado\Desktop\SQL-project4\SQL-Data-Warehouse-Project\data_sets\source_erp\';
BEGIN
    v_batch_start := CLOCK_TIMESTAMP();

    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Starting bronze layer load';
    RAISE NOTICE '=======================================';

    -- Loading CRM tables
    RAISE NOTICE ' ';
    RAISE NOTICE '--- Loading CRM tables --- ';

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_cust_info';

    v_start_time := CLOCK_TIMESTAMP();
    -- Delete if any rows
    TRUNCATE TABLE bronze.crm_cust_info;
    -- Load data from csv
    EXECUTE format('COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'cust_info.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'LOADED % ROWS IN %', v_rows_affected, v_duration;

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_prd_info';

    v_start_time := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE format('COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'prd_info.csv', ',');

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % ROWS IN %', v_rows_affected, v_duration;

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: crm_sales_details';
    v_start_time := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE format('COPY bronze.crm_sales_details FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_crm || 'sales_details.csv', ',');
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'Loaded % ROWS IN %', v_rows_affected, v_duration;

    RAISE NOTICE ' ';
    RAISE NOTICE '--- Loading ERP tables ---';

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: erp_cust_az12';

    v_start_time := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE format('COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'cust_az12.csv', ',');
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'LOADED % ROWS IN %', v_rows_affected, v_duration;

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: bronze.erp_loc_a101';

    v_start_time := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE format('COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'loc_a101.csv', ',');
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();

    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);
    RAISE NOTICE 'LOADED % ROWS IN %', v_rows_affected, v_duration;

    RAISE NOTICE ' ';
    RAISE NOTICE 'Loading: bronze.erp_px_cat_g1v2';

    v_start_time := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE format('COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L)',
                   v_base_path_erp || 'px_cat_g1v2.csv', ',');
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := CLOCK_TIMESTAMP();
    v_duration := ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2);

    RAISE NOTICE 'LOADED % ROWS IN %', v_rows_affected, v_duration;

    -- Summary
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

CALL bronze.load_bronze();