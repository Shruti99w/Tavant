-- Databricks notebook source
-- 1. Create a catalog
CREATE CATALOG IF NOT EXISTS dp700_demo;

-- 2. Create a schema
CREATE SCHEMA IF NOT EXISTS dp700_demo.dp700_e016;

-- 3. Create a table with sample data
CREATE OR REPLACE TABLE dp700_demo.dp700_e016.customer (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_at DATE
);

-- 4. Insert sample data
INSERT INTO dp700_demo.dp700_e016.customer VALUES
(1, 'Alice', 'Johnson', 'alice.johnson@example.com', '2024-01-10'),
(2, 'Bob', 'Smith', 'bob.smith@example.com', '2024-02-15'),
(3, 'Charlie', 'Lee', 'charlie.lee@example.com', '2024-03-20');

-- 5. Create view
CREATE OR REPLACE VIEW dp700_demo.dp700_e016.customer_view AS
SELECT
  customer_id,
  CONCAT(first_name, ' ', last_name) AS full_name,
  email
FROM
  dp700_demo.dp700_e016.customer;

-- COMMAND ----------

select * from `dp700_e016`.`customer` limit 100;