-- Exploring and checking data for duplicates
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
    OR cst_id IS NULL;

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = '29466';

SELECT *, row_number() over (partition by cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info;

-- Check for unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Actual transformations.
SELECT cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname), cst_marital_status, cst_gndr, cst_create_date
FROM (SELECT *, row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_list
      FROM bronze.crm_cust_info
      WHERE cst_id IS NOT NULL) t
WHERE flag_list = 1;



