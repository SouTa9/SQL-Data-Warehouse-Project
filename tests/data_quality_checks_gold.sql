/*
================================================================================
Script:     data_quality_checks_gold.sql
Purpose:    Comprehensive data quality validation for Gold layer
Layer:      Gold
================================================================================

35 automated tests across 4 categories:
    1. Customer dimension checks (9 tests)
    2. Product dimension checks (10 tests)
    3. Sales fact checks (12 tests)
    4. Cross-dimensional integrity (4 tests)

Quality dimensions tested:
    ✓ Completeness, Accuracy, Consistency
    ✓ Validity, Integrity, Uniqueness

Expected results:
    - NULL checks: 0 rows
    - Duplicate checks: 0 rows
    - Referential integrity: 0 orphans
    - Data completeness: >95%

Execution:
    Run AFTER: Gold views created and Silver loaded
    Run BEFORE: BI tool connections

================================================================================
*/

-- ============================================================================
-- Gold Layer - Data Quality Checks
-- ============================================================================
-- Description: Quality validation tests for gold layer views
-- ============================================================================

-- ============================================================================
-- Section 1: Dimension Customer Quality Checks
-- ============================================================================

-- Test 1.1: Check source data availability
-- Expected: Should return rows from both source tables
SELECT 'Source Data Check - CRM Customer Info' AS test_name,
       COUNT(*) AS record_count
FROM silver.crm_cust_info;

SELECT 'Source Data Check - ERP Customer AZ12' AS test_name,
       COUNT(*) AS record_count
FROM silver.erp_cust_az12;

-- Test 1.2: Preview dim_customer data
-- Expected: Should return customer records with all fields populated
SELECT 'dim_customer Preview' AS test_name,
       *
FROM gold.dim_customer
LIMIT 10;

-- Test 1.3: Check for NULL customer_key (should be 0)
-- Expected: 0 rows with NULL customer_key
SELECT 'NULL customer_key Check' AS test_name,
       COUNT(*) AS null_count
FROM gold.dim_customer
WHERE customer_key IS NULL;

-- Test 1.4: Check for duplicate customer_key (should be 0)
-- Expected: 0 duplicate customer keys
SELECT 'Duplicate customer_key Check' AS test_name,
       customer_key,
       COUNT(*) AS duplicate_count
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Test 1.5: Check gender distribution
-- Expected: Should show all gender values and their counts
SELECT 'Gender Distribution' AS test_name,
       gender,
       COUNT(*) AS count
FROM gold.dim_customer
GROUP BY gender
ORDER BY count DESC;

-- Test 1.6: Validate gender logic
-- Expected: Should show how gender values are resolved between CRM and ERP
SELECT DISTINCT
       'Gender Logic Validation' AS test_name,
       ci.cst_gndr AS crm_gender,
       e.gen AS erp_gender,
       CASE
           WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
           ELSE COALESCE(e.gen, 'n/a')
       END AS resolved_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 e ON ci.cst_key = e.cid
ORDER BY 2, 3;

-- Test 1.7: Check for NULL critical fields
-- Expected: Should return counts of NULL values for each field
SELECT 'NULL Values in Critical Fields' AS test_name,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
       SUM(CASE WHEN customer_number IS NULL THEN 1 ELSE 0 END) AS null_customer_number,
       SUM(CASE WHEN firstname IS NULL THEN 1 ELSE 0 END) AS null_firstname,
       SUM(CASE WHEN lastname IS NULL THEN 1 ELSE 0 END) AS null_lastname
FROM gold.dim_customer;

-- Test 1.8: Check country distribution
-- Expected: Should show all countries and their counts
SELECT 'Country Distribution' AS test_name,
       country,
       COUNT(*) AS count
FROM gold.dim_customer
GROUP BY country
ORDER BY count DESC;

-- Test 1.9: Check marital status distribution
-- Expected: Should show all marital statuses and their counts
SELECT 'Marital Status Distribution' AS test_name,
       marital_status,
       COUNT(*) AS count
FROM gold.dim_customer
GROUP BY marital_status
ORDER BY count DESC;

-- ============================================================================
-- Section 2: Dimension Products Quality Checks
-- ============================================================================

-- Test 2.1: Check source data availability
-- Expected: Should return rows from both source tables
SELECT 'Source Data Check - CRM Product Info' AS test_name,
       COUNT(*) AS record_count
FROM silver.crm_prd_info;

SELECT 'Source Data Check - ERP Product Category' AS test_name,
       COUNT(*) AS record_count
FROM silver.erp_px_cat_g1v2;

-- Test 2.2: Preview dim_products data
-- Expected: Should return product records with all fields populated
SELECT 'dim_products Preview' AS test_name,
       *
FROM gold.dim_products
LIMIT 10;

-- Test 2.3: Check for NULL product_key (should be 0)
-- Expected: 0 rows with NULL product_key
SELECT 'NULL product_key Check' AS test_name,
       COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_key IS NULL;

-- Test 2.4: Check for duplicate product_number
-- Expected: 0 duplicate product numbers
SELECT 'Duplicate product_number Check' AS test_name,
       product_number,
       COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_number
HAVING COUNT(*) > 1;

-- Test 2.5: Verify only current products are included (prd_end_dt IS NULL)
-- Expected: Should show the filtering logic is working
SELECT 'Current Products Validation' AS test_name,
       SUM(CASE WHEN prd_end_dt IS NULL THEN 1 ELSE 0 END) AS current_products,
       SUM(CASE WHEN prd_end_dt IS NOT NULL THEN 1 ELSE 0 END) AS historical_products
FROM silver.crm_prd_info;

-- Test 2.6: Check category distribution
-- Expected: Should show all categories and their counts
SELECT 'Category Distribution' AS test_name,
       category,
       COUNT(*) AS count
FROM gold.dim_products
GROUP BY category
ORDER BY count DESC;

-- Test 2.7: Check subcategory distribution
-- Expected: Should show all subcategories and their counts
SELECT 'Subcategory Distribution' AS test_name,
       subcategory,
       COUNT(*) AS count
FROM gold.dim_products
GROUP BY subcategory
ORDER BY count DESC;

-- Test 2.8: Check product line distribution
-- Expected: Should show all product lines and their counts
SELECT 'Product Line Distribution' AS test_name,
       product_line,
       COUNT(*) AS count
FROM gold.dim_products
GROUP BY product_line
ORDER BY count DESC;

-- Test 2.9: Check for NULL critical fields
-- Expected: Should return counts of NULL values for each field
SELECT 'NULL Values in Critical Fields' AS test_name,
       SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
       SUM(CASE WHEN product_number IS NULL THEN 1 ELSE 0 END) AS null_product_number,
       SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS null_product_name,
       SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category
FROM gold.dim_products;

-- Test 2.10: Check product cost statistics
-- Expected: Should show min, max, avg product costs
SELECT 'Product Cost Statistics' AS test_name,
       MIN(product_cost) AS min_cost,
       MAX(product_cost) AS max_cost,
       AVG(product_cost) AS avg_cost,
       COUNT(*) AS total_products
FROM gold.dim_products;

-- ============================================================================
-- Section 3: Fact Sales Quality Checks
-- ============================================================================

-- Test 3.1: Check source data availability
-- Expected: Should return rows from source table
SELECT 'Source Data Check - CRM Sales Details' AS test_name,
       COUNT(*) AS record_count
FROM silver.crm_sales_details;

-- Test 3.2: Preview fact_sales data
-- Expected: Should return sales records with all fields populated
SELECT 'fact_sales Preview' AS test_name,
       *
FROM gold.fact_sales
LIMIT 10;

-- Test 3.3: Check for NULL surrogate keys
-- Expected: Should show how many sales records have NULL foreign keys
SELECT 'NULL Surrogate Keys Check' AS test_name,
       SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key
FROM gold.fact_sales;

-- Test 3.4: Validate joins with dimensions
-- Expected: Should show how many sales match with customers and products
SELECT 'Dimension Join Validation' AS test_name,
       COUNT(*) AS total_sales,
       SUM(CASE WHEN c.customer_key IS NOT NULL THEN 1 ELSE 0 END) AS sales_with_customer,
       SUM(CASE WHEN p.product_key IS NOT NULL THEN 1 ELSE 0 END) AS sales_with_product
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_customer AS c ON c.customer_id = s.sls_cust_id
LEFT JOIN gold.dim_products AS p ON p.product_number = s.sls_prd_key;

-- Test 3.5: Check for duplicate order_number
-- Expected: Should show if there are any duplicate orders
SELECT 'Duplicate Order Number Check' AS test_name,
       order_number,
       COUNT(*) AS duplicate_count
FROM gold.fact_sales
GROUP BY order_number
HAVING COUNT(*) > 1
LIMIT 10;

-- Test 3.6: Check sales amount statistics
-- Expected: Should show min, max, avg, total sales
SELECT 'Sales Amount Statistics' AS test_name,
       MIN(sales) AS min_sales,
       MAX(sales) AS max_sales,
       AVG(sales) AS avg_sales,
       SUM(sales) AS total_sales,
       COUNT(*) AS total_transactions
FROM gold.fact_sales;

-- Test 3.7: Check quantity statistics
-- Expected: Should show min, max, avg quantities
SELECT 'Quantity Statistics' AS test_name,
       MIN(quantity) AS min_quantity,
       MAX(quantity) AS max_quantity,
       AVG(quantity) AS avg_quantity
FROM gold.fact_sales;

-- Test 3.8: Check for negative values (data quality issue)
-- Expected: Should be 0 for all fields
SELECT 'Negative Values Check' AS test_name,
       SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END) AS negative_sales,
       SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantity,
       SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS negative_price
FROM gold.fact_sales;

-- Test 3.9: Check date validity
-- Expected: Should show date ranges and identify any invalid dates
SELECT 'Date Range Check' AS test_name,
       MIN(order_date) AS earliest_order_date,
       MAX(order_date) AS latest_order_date,
       MIN(shipping_date) AS earliest_shipping_date,
       MAX(shipping_date) AS latest_shipping_date,
       MIN(due_date) AS earliest_due_date,
       MAX(due_date) AS latest_due_date
FROM gold.fact_sales;

-- Test 3.10: Check date logic (shipping should be >= order date)
-- Expected: Should be 0 violations
SELECT 'Date Logic Validation' AS test_name,
       SUM(CASE WHEN shipping_date < order_date THEN 1 ELSE 0 END) AS shipping_before_order,
       SUM(CASE WHEN due_date < order_date THEN 1 ELSE 0 END) AS due_before_order
FROM gold.fact_sales;

-- Test 3.11: Sales by customer (top 10)
-- Expected: Should show top customers by sales volume
SELECT 'Top 10 Customers by Sales' AS test_name,
       customer_key,
       COUNT(*) AS transaction_count,
       SUM(sales) AS total_sales
FROM gold.fact_sales
WHERE customer_key IS NOT NULL
GROUP BY customer_key
ORDER BY total_sales DESC
LIMIT 10;

-- Test 3.12: Sales by product (top 10)
-- Expected: Should show top products by sales volume
SELECT 'Top 10 Products by Sales' AS test_name,
       product_key,
       COUNT(*) AS transaction_count,
       SUM(sales) AS total_sales
FROM gold.fact_sales
WHERE product_key IS NOT NULL
GROUP BY product_key
ORDER BY total_sales DESC
LIMIT 10;

-- ============================================================================
-- Section 4: Cross-Dimensional Quality Checks
-- ============================================================================

-- Test 4.1: Record count summary
-- Expected: Should show record counts for all gold layer views
SELECT 'Record Count Summary' AS test_name,
       (SELECT COUNT(*) FROM gold.dim_customer) AS customer_count,
       (SELECT COUNT(*) FROM gold.dim_products) AS product_count,
       (SELECT COUNT(*) FROM gold.fact_sales) AS sales_count;

-- Test 4.2: Referential integrity check - Products
-- Expected: Should be 0 orphaned products in fact_sales
SELECT 'Orphaned Products in fact_sales' AS test_name,
       COUNT(*) AS orphaned_count
FROM gold.fact_sales f
WHERE f.product_key NOT IN (SELECT product_key FROM gold.dim_products WHERE product_key IS NOT NULL);

-- Test 4.3: Referential integrity check - Customers
-- Expected: Should be 0 orphaned customers in fact_sales
SELECT 'Orphaned Customers in fact_sales' AS test_name,
       COUNT(*) AS orphaned_count
FROM gold.fact_sales f
WHERE f.customer_key NOT IN (SELECT customer_key FROM gold.dim_customer WHERE customer_key IS NOT NULL);

-- Test 4.4: Data completeness check
-- Expected: Percentage of sales with complete dimension references
SELECT 'Data Completeness Check' AS test_name,
       COUNT(*) AS total_sales,
       SUM(CASE WHEN product_key IS NOT NULL AND customer_key IS NOT NULL THEN 1 ELSE 0 END) AS complete_records,
       ROUND(100.0 * SUM(CASE WHEN product_key IS NOT NULL AND customer_key IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS completeness_percentage
FROM gold.fact_sales;

-- ============================================================================
-- End of Gold Layer Quality Checks
-- ============================================================================

