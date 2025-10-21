# Data Catalog

> Comprehensive data dictionary for the SQL Data Warehouse Project

## Overview

**Architecture:** Medallion (Bronze → Silver → Gold)  
**Database:** `data_warehouse`  
**Total Tables/Views:** 15 (6 Bronze + 6 Silver + 3 Gold)

---

## Gold Layer - Star Schema

### 🌟 dim_customer

**Purpose:** Unified customer dimension integrating CRM and ERP data  
**Grain:** One row per customer  
**Source:** CRM customer info + ERP demographics + locations

| Column          | Type        | Description                            |
|-----------------|-------------|----------------------------------------|
| customer_key    | BIGINT      | Surrogate key (PK)                     |
| customer_id     | INT         | Natural key from CRM                   |
| customer_number | VARCHAR(50) | Business key (AW00000001 format)       |
| firstname       | VARCHAR(50) | Customer first name                    |
| lastname        | VARCHAR(50) | Customer last name                     |
| birthdate       | DATE        | Date of birth (from ERP)               |
| country         | VARCHAR(50) | Country of residence                   |
| marital_status  | VARCHAR(50) | Married, Single, or n/a                |
| gender          | VARCHAR(50) | Male, Female, or n/a (CRM prioritized) |
| creation_date   | DATE        | Account creation date                  |

**Business Rules:**

- Gender resolution: CRM value takes precedence over ERP
- Approximately 700 unique customers

---

### 🌟 dim_products

**Purpose:** Product catalog with category hierarchy (active products only)  
**Grain:** One row per active product  
**Source:** CRM product info + ERP categories

| Column         | Type        | Description                            |
|----------------|-------------|----------------------------------------|
| product_key    | BIGINT      | Surrogate key (PK)                     |
| product_id     | INT         | Natural key from CRM                   |
| product_number | VARCHAR(50) | SKU/product code                       |
| product_name   | VARCHAR(50) | Product description                    |
| category_id    | INT         | Category identifier                    |
| category       | VARCHAR(50) | Top-level category (Bikes, Components) |
| subcategory    | VARCHAR(50) | Detailed classification                |
| maintenance    | VARCHAR(50) | Requires maintenance (Yes/No)          |
| product_cost   | INT         | Standard cost                          |
| product_line   | VARCHAR(50) | Road, Mountain, Touring, Standard      |
| start_date     | DATE        | Product activation date                |

**Business Rules:**

- Only active products included (prd_end_dt IS NULL)
- Approximately 200 active products

---

### 📊 fact_sales

**Purpose:** Sales transactions with dimension keys  
**Grain:** One row per order line item  
**Source:** CRM sales details + dimension lookups

| Column        | Type        | Measure Type  | Description            |
|---------------|-------------|---------------|------------------------|
| order_number  | VARCHAR(50) | Dimension     | Sales order identifier |
| product_key   | BIGINT      | FK            | → dim_products         |
| customer_key  | BIGINT      | FK            | → dim_customer         |
| order_date    | DATE        | Dimension     | Order placement date   |
| shipping_date | DATE        | Dimension     | Shipment date          |
| due_date      | DATE        | Dimension     | Expected delivery date |
| sales         | INT         | Additive      | Total revenue amount   |
| quantity      | INT         | Additive      | Units sold             |
| price         | INT         | Semi-additive | Unit price             |

**Business Rules:**

- Approximately 60,000 transactions
- Sales validation: sales = price × quantity
- Date logic: order_date ≤ shipping_date ≤ due_date

---

## Silver Layer - Cleansed Data

### Transformation Summary

| Table             | Key Transformations                                     |
|-------------------|---------------------------------------------------------|
| crm_cust_info     | Deduplication, gender/status standardization, trimming  |
| crm_prd_info      | Product line standardization, SCD Type 2, cost handling |
| crm_sales_details | Date type conversion (INT→DATE), validation             |
| erp_cust_az12     | Date conversion, customer ID formatting                 |
| erp_loc_a101      | Country standardization, trimming                       |
| erp_px_cat_g1v2   | Category hierarchy normalization                        |

**Common Columns Added:**

- `dwh_create_date` (TIMESTAMP): Audit timestamp for warehouse load

---

## Bronze Layer - Raw Data

### Source Systems

**CRM (Customer Relationship Management):**

- `crm_cust_info`: Customer master (~700 records)
- `crm_prd_info`: Product catalog (~300 records including historical)
- `crm_sales_details`: Sales transactions (~60,000 records)

**ERP (Enterprise Resource Planning):**

- `erp_cust_az12`: Customer demographics (~700 records)
- `erp_loc_a101`: Geographic locations (~700 records)
- `erp_px_cat_g1v2`: Product categories (4 records)

**Characteristics:**

- No transformations applied
- All columns nullable
- Exact replica of CSV source files
- Full truncate-and-load strategy

---

## Data Lineage

```
CSV Files
    ↓
Bronze Layer (Raw staging)
    ↓
Silver Layer (Cleansed + validated)
    ↓
Gold Layer (Star schema for BI)
```

**Key Integration Points:**

- Customer: CRM cust_info + ERP demographics + locations → dim_customer
- Product: CRM prd_info + ERP categories → dim_products
- Sales: CRM sales_details + dimension keys → fact_sales

---

## Data Quality

**Validation Levels:**

- Bronze: No validation (accept as-is)
- Silver: 20+ checks (duplicates, NULLs, data types)
- Gold: 35+ checks (completeness, accuracy, referential integrity)

**Test Categories:**

- Completeness, Accuracy, Consistency, Validity, Integrity, Uniqueness

---

## Naming Conventions

**Tables:** `<layer>.<source>_<entity>` (e.g., `silver.crm_cust_info`)  
**Views:** `<layer>.<category>_<entity>` (e.g., `gold.dim_customer`)  
**Columns:** snake_case with source prefixes (e.g., `cst_id`, `prd_key`)  
**Keys:** `<entity>_key` for surrogate keys, `<entity>_id` for natural keys

---

*For detailed column definitions and business rules, see inline SQL comments in `scripts/` directory.*

