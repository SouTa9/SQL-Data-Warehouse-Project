/*
================================================================================
Script:     data_quality_checks_gold.sql
Purpose:    Comprehensive data quality validation for Gold layer
Layer:      Gold
Usage:      Run manually in a SQL client after Gold views are created
            (these checks are also enforced in Airflow with assertions)
================================================================================
*/

-- ============================================================================
-- Section 1: Dimension Customer Quality Checks
-- ============================================================================

-- Source availability
SELECT 'Source Data Check - CRM Customer Info' AS test_name, COUNT(*) AS record_count FROM silver.crm_cust_info;
SELECT 'Source Data Check - ERP Customer AZ12' AS test_name, COUNT(*) AS record_count FROM silver.erp_cust_az12;

-- Preview dim_customers
SELECT 'dim_customers Preview' AS test_name, * FROM gold.dim_customers LIMIT 10;

-- No NULL surrogate keys
SELECT 'NULL customer_key count' AS test_name, COUNT(*) AS null_keys FROM gold.dim_customers WHERE customer_key IS NULL;

-- No duplicate natural IDs
SELECT 'Duplicate customers by customer_id' AS test_name, customer_id, COUNT(*) AS duplicate_count FROM gold.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1;

-- Gender validation and distribution
SELECT 'Invalid gender values' AS test_name, gender
FROM gold.dim_customers
WHERE gender NOT IN ('Male', 'Female', 'n/a')
GROUP BY gender;
SELECT 'Gender Distribution' AS test_name, gender, COUNT(*) AS count FROM gold.dim_customers GROUP BY gender ORDER BY count DESC;

-- NULLs in key descriptive fields
SELECT 'NULL Values in Critical Fields' AS test_name,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
       SUM(CASE WHEN customer_number IS NULL THEN 1 ELSE 0 END) AS null_customer_number,
       SUM(CASE WHEN firstname IS NULL THEN 1 ELSE 0 END) AS null_firstname,
       SUM(CASE WHEN lastname IS NULL THEN 1 ELSE 0 END) AS null_lastname
FROM gold.dim_customers;

-- Country / Marital status distributions
SELECT 'Country Distribution' AS test_name, country, COUNT(*) AS count FROM gold.dim_customers GROUP BY country ORDER BY count DESC;
SELECT 'Marital Status Distribution' AS test_name, marital_status, COUNT(*) AS count FROM gold.dim_customers GROUP BY marital_status ORDER BY count DESC;

-- ============================================================================
-- Section 2: Dimension Products Quality Checks
-- ============================================================================

-- Source availability
SELECT 'Source Data Check - CRM Product Info' AS test_name, COUNT(*) AS record_count FROM silver.crm_prd_info;
SELECT 'Source Data Check - ERP Product Category' AS test_name, COUNT(*) AS record_count FROM silver.erp_px_cat_g1v2;

-- Preview dim_products
SELECT 'dim_products Preview' AS test_name, * FROM gold.dim_products LIMIT 10;

-- No NULL product_key
SELECT 'NULL product_key count' AS test_name, COUNT(*) AS null_count FROM gold.dim_products WHERE product_key IS NULL;

-- No duplicate product_number
SELECT 'Duplicate product_number Check' AS test_name, product_number, COUNT(*) AS duplicate_count FROM gold.dim_products GROUP BY product_number HAVING COUNT(*) > 1;

-- Current vs historical products (from silver)
SELECT 'Current Products Validation' AS test_name,
       SUM(CASE WHEN prd_end_dt IS NULL THEN 1 ELSE 0 END) AS current_products,
       SUM(CASE WHEN prd_end_dt IS NOT NULL THEN 1 ELSE 0 END) AS historical_products
FROM silver.crm_prd_info;

-- Category/Subcategory/Product line distributions
SELECT 'Category Distribution' AS test_name, category, COUNT(*) AS count FROM gold.dim_products GROUP BY category ORDER BY count DESC;
SELECT 'Subcategory Distribution' AS test_name, subcategory, COUNT(*) AS count FROM gold.dim_products GROUP BY subcategory ORDER BY count DESC;
SELECT 'Product Line Distribution' AS test_name, product_line, COUNT(*) AS count FROM gold.dim_products GROUP BY product_line ORDER BY count DESC;

-- NULLs in critical product fields
SELECT 'NULL Values in Critical Fields' AS test_name,
       SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
       SUM(CASE WHEN product_number IS NULL THEN 1 ELSE 0 END) AS null_product_number,
       SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS null_product_name,
       SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category
FROM gold.dim_products;

-- Product cost stats
SELECT 'Product Cost Statistics' AS test_name,
       MIN(product_cost) AS min_cost,
       MAX(product_cost) AS max_cost,
       AVG(product_cost) AS avg_cost,
       COUNT(*) AS total_products
FROM gold.dim_products;

-- ============================================================================
-- Section 3: Fact Sales Quality Checks
-- ============================================================================

-- Source availability
SELECT 'Source Data Check - CRM Sales Details' AS test_name, COUNT(*) AS record_count FROM silver.crm_sales_details;

-- Preview fact_sales
SELECT 'fact_sales Preview' AS test_name, * FROM gold.fact_sales LIMIT 10;

-- NULL surrogate keys in fact
SELECT 'NULL Surrogate Keys Check' AS test_name,
       SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key
FROM gold.fact_sales;

-- Dimension join validation (from source to dims)
SELECT 'Dimension Join Validation' AS test_name,
       COUNT(*) AS total_sales,
       SUM(CASE WHEN c.customer_key IS NOT NULL THEN 1 ELSE 0 END) AS sales_with_customer,
       SUM(CASE WHEN p.product_key IS NOT NULL THEN 1 ELSE 0 END) AS sales_with_product
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_customers AS c ON c.customer_id = s.sls_cust_id
LEFT JOIN gold.dim_products  AS p ON p.product_number = s.sls_prd_key;

-- Duplicate order numbers
SELECT 'Duplicate Order Number Check' AS test_name, order_number, COUNT(*) AS duplicate_count
FROM gold.fact_sales
GROUP BY order_number
HAVING COUNT(*) > 1
LIMIT 10;

-- Sales and quantity stats
SELECT 'Sales Amount Statistics' AS test_name, MIN(sales) AS min_sales, MAX(sales) AS max_sales, AVG(sales) AS avg_sales, SUM(sales) AS total_sales, COUNT(*) AS total_transactions FROM gold.fact_sales;
SELECT 'Quantity Statistics' AS test_name, MIN(quantity) AS min_quantity, MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity FROM gold.fact_sales;

-- No negative values
SELECT 'Negative Values Check' AS test_name,
       SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END) AS negative_sales,
       SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantity,
       SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS negative_price
FROM gold.fact_sales;

-- Date ranges and logic
SELECT 'Date Range Check' AS test_name, MIN(order_date) AS earliest_order_date, MAX(order_date) AS latest_order_date, MIN(shipping_date) AS earliest_shipping_date, MAX(shipping_date) AS latest_shipping_date, MIN(due_date) AS earliest_due_date, MAX(due_date) AS latest_due_date FROM gold.fact_sales;
SELECT 'Date Logic Validation' AS test_name, SUM(CASE WHEN shipping_date < order_date THEN 1 ELSE 0 END) AS shipping_before_order, SUM(CASE WHEN due_date < order_date THEN 1 ELSE 0 END) AS due_before_order FROM gold.fact_sales;

-- Top customers/products by sales
SELECT 'Top 10 Customers by Sales' AS test_name, customer_key, COUNT(*) AS transaction_count, SUM(sales) AS total_sales FROM gold.fact_sales WHERE customer_key IS NOT NULL GROUP BY customer_key ORDER BY total_sales DESC LIMIT 10;
SELECT 'Top 10 Products by Sales' AS test_name, product_key, COUNT(*) AS transaction_count, SUM(sales) AS total_sales FROM gold.fact_sales WHERE product_key IS NOT NULL GROUP BY product_key ORDER BY total_sales DESC LIMIT 10;

-- ============================================================================
-- Section 4: Cross-Dimensional Quality Checks
-- ============================================================================

-- Record count summary
SELECT 'Record Count Summary' AS test_name,
       (SELECT COUNT(*) FROM gold.dim_customers) AS customer_count,
       (SELECT COUNT(*) FROM gold.dim_products)  AS product_count,
       (SELECT COUNT(*) FROM gold.fact_sales)    AS sales_count;

-- Orphan checks
SELECT 'Orphaned Products in fact_sales' AS test_name, COUNT(*) AS orphaned_count
FROM gold.fact_sales f
WHERE f.product_key NOT IN (SELECT product_key FROM gold.dim_products WHERE product_key IS NOT NULL);

SELECT 'Orphaned Customers in fact_sales' AS test_name, COUNT(*) AS orphaned_count
FROM gold.fact_sales f
WHERE f.customer_key NOT IN (SELECT customer_key FROM gold.dim_customers WHERE customer_key IS NOT NULL);

-- Completeness
SELECT 'Data Completeness Check' AS test_name,
       COUNT(*) AS total_sales,
       SUM(CASE WHEN product_key IS NOT NULL AND customer_key IS NOT NULL THEN 1 ELSE 0 END) AS complete_records,
       ROUND(100.0 * SUM(CASE WHEN product_key IS NOT NULL AND customer_key IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS completeness_percentage
FROM gold.fact_sales;
