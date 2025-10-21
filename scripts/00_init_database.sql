/*
================================================================================
Script:     00_init_database.sql
Purpose:    Initialize database and Medallion Architecture schemas
Layer:      Infrastructure
================================================================================

Creates:
    1. Database: data_warehouse
    2. Schemas: bronze (raw), silver (cleansed), gold (analytics)

Execution:
    - Run FIRST before any other scripts
    - Requires: PostgreSQL 12+, database creation privileges
    - Idempotent: Safe to run multiple times

Next Steps:
    1. Connect to data_warehouse database
    2. Run Bronze layer scripts

================================================================================
*/

-- ============================================================================
-- DATABASE CREATION
-- ============================================================================
-- Uncomment to drop existing database (⚠️ WARNING: This will delete all data)
-- DROP DATABASE IF EXISTS data_warehouse;

-- Create the main data warehouse database
CREATE DATABASE data_warehouse;

-- ============================================================================
-- SCHEMA CREATION - MEDALLION ARCHITECTURE
-- ============================================================================

-- Bronze Schema: Raw data from source systems (CRM, ERP)
-- Purpose: Landing zone for unprocessed data, maintains source system formats
CREATE SCHEMA IF NOT EXISTS bronze;

-- Silver Schema: Cleansed, validated, and standardized data
-- Purpose: Trusted data layer with quality rules applied, ready for analytics
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold Schema: Business-level aggregates and dimension models
-- Purpose: Presentation layer optimized for BI tools and reporting
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Verify schemas were created successfully
-- SELECT schema_name FROM information_schema.schemata
-- WHERE schema_name IN ('bronze', 'silver', 'gold');
