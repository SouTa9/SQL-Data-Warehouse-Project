/*
================================================================================
Script:     ddl_gold.sql
Purpose:    Create Gold layer star schema views
Layer:      Gold
================================================================================

Creates 3 views implementing star schema:
    - dim_customer: Customer dimension (CRM + ERP integrated)
    - dim_products: Product dimension with categories (active only)
    - fact_sales: Sales transactions with dimension keys

Design:
    - Star schema for optimal analytics
    - Surrogate keys (ROW_NUMBER) for performance
    - Multi-source integration with business rules
    - Views provide real-time data from Silver

Execution:
    Run AFTER: silver.load_silver()
    Run BEFORE: Data quality checks or BI tool connections

================================================================================
*/

-- ============================================================================
-- DIMENSION: CUSTOMER
-- ============================================================================
-- Business Purpose:
--   Provides a unified view of customer information by integrating CRM customer
--   master data with ERP demographic and location data. Used for customer
--   segmentation, demographic analysis, and personalization.
--
-- Grain: One row per unique customer (identified by cst_id)
--
-- Source Integration:
--   - Primary: silver.crm_cust_info (customer master)
--   - Secondary: silver.erp_cust_az12 (demographics like birthdate, gender)
--   - Secondary: silver.erp_loc_a101 (geographic information)
--
-- Key Transformations:
--   - Surrogate key generation for join optimization
--   - Gender resolution (CRM takes precedence over ERP)
--   - Multi-source attribute merging via LEFT JOINs
--
-- Row Count: ~700 customers (varies by source data)
-- ============================================================================

CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
       ci.cst_id                           AS customer_id,
       ci.cst_key                          AS customer_number,
       ci.cst_firstname                    AS firstname,
       ci.cst_lastname                     AS lastname,
       e.bdate                             AS birthdate,
       el.cntry                            AS country,
       ci.cst_marital_status               AS marital_status,
       CASE
           WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
           ELSE COALESCE(e.gen, 'n/a')
           END                             AS gender,
       ci.cst_create_date                  AS creation_date
FROM silver.crm_cust_info AS ci
         LEFT JOIN silver.erp_cust_az12 AS e ON ci.cst_key = e.cid
         LEFT JOIN silver.erp_loc_a101 AS el ON el.cid = ci.cst_key;

-- ============================================================================
-- DIMENSION: PRODUCTS
-- ============================================================================
-- Business Purpose:
--   Provides current product catalog with hierarchical category information
--   for product performance analysis, inventory management, and catalog insights.
--
-- Grain: One row per unique active product (identified by prd_key)
--
-- Source Integration:
--   - Primary: silver.crm_prd_info (product master)
--   - Secondary: silver.erp_px_cat_g1v2 (category hierarchy)
--
-- Key Transformations:
--   - Surrogate key generation for join optimization
--   - Filtering to active products only (WHERE prd_end_dt IS NULL)
--   - Category hierarchy integration (category → subcategory)
--
-- SCD Type: Type 0 (current state only, no history)
-- Row Count: ~200 active products (varies by source data)
-- ============================================================================

CREATE OR REPLACE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER (ORDER BY ci.prd_start_dt, ci.prd_key) AS product_key,
       ci.prd_id                                                AS product_id,
       ci.prd_key                                               AS product_number,
       ci.prd_nm                                                AS product_name,
       ci.cat_id                                                AS category_id,
       eg.cat                                                   AS category,
       eg.subcat                                                AS subcategory,
       eg.maintenance,
       ci.prd_cost                                              AS product_cost,
       ci.prd_line                                              AS product_line,
       ci.prd_start_dt                                          AS start_date
FROM silver.crm_prd_info AS ci
         LEFT JOIN silver.erp_px_cat_g1v2 AS eg ON eg.id = ci.cat_id
WHERE prd_end_dt IS NULL;

-- ============================================================================
-- FACT: SALES
-- ============================================================================
-- Business Purpose:
--   Central fact table containing all sales transactions with links to
--   customer and product dimensions. Supports revenue analysis, sales trends,
--   customer behavior analytics, and product performance reporting.
--
-- Grain: One row per sales order line item (order_number + product + customer)
--
-- Fact Type: Transactional (captures individual sale events)
--
-- Measures (Numeric):
--   - sales: Total sale amount (additive)
--   - quantity: Units sold (additive)
--   - price: Unit price (semi-additive)
--
-- Dimensions:
--   - customer_key → dim_customers (who bought)
--   - product_key → dim_products (what was bought)
--   - order_date, shipping_date, due_date (when it happened)
--
-- Source Integration:
--   - Primary: silver.crm_sales_details (transaction data)
--   - Lookup: gold.dim_customers (surrogate keys)
--   - Lookup: gold.dim_products (surrogate keys)
--
-- Key Transformations:
--   - Surrogate key lookups via LEFT JOIN
--   - Business-friendly column renaming
--   - Maintains all historical transactions
--
-- Row Count: ~60,000 transactions (varies by source data)
-- ============================================================================

CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT sls_ord_num  AS order_number,
       p.product_key,
       c.customer_key,
       sls_order_dt AS order_date,
       sls_ship_dt  AS shipping_date,
       sls_due_dt   AS due_date,
       sls_sales    AS sales,
       sls_quantity AS quantity,
       sls_price    AS price
FROM silver.crm_sales_details AS s
         LEFT JOIN gold.dim_customers AS c ON c.customer_id = s.sls_cust_id
       LEFT JOIN gold.dim_products AS p ON p.product_number = s.sls_prd_key; -- Equality join now that silver.crm_prd_info.prd_key is shortened to match sales

-- ============================================================================
-- POST-CREATION VALIDATION
-- ============================================================================
-- Uncomment to verify views were created successfully:
-- SELECT table_name, table_type
-- FROM information_schema.tables
-- WHERE table_schema = 'gold'
-- ORDER BY table_name;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================
-- Example 1: Total sales by customer
-- SELECT c.firstname, c.lastname, SUM(f.sales) as total_sales
-- FROM gold.fact_sales f
-- JOIN gold.dim_customers c ON f.customer_key = c.customer_key
-- GROUP BY c.customer_key, c.firstname, c.lastname
-- ORDER BY total_sales DESC
-- LIMIT 10;

-- Example 2: Sales by product category
-- SELECT p.category, p.subcategory,
--        COUNT(*) as transactions,
--        SUM(f.sales) as total_revenue
-- FROM gold.fact_sales f
-- JOIN gold.dim_products p ON f.product_key = p.product_key
-- GROUP BY p.category, p.subcategory
-- ORDER BY total_revenue DESC;

-- ============================================================================
-- End of Gold Layer DDL
-- ============================================================================
