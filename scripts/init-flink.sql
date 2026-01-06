-- ============================================
-- Flink SQL Initialization Script
-- Creates sample tables for testing Metabase
-- Uses filesystem connector with CSV files
-- ============================================

-- Create users table from CSV
CREATE TABLE IF NOT EXISTS users (
    user_id INT,
    username STRING,
    email STRING,
    age INT,
    country STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/users.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
);

-- Create orders table from CSV
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10, 2),
    status STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/orders.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
);

-- Create products table from CSV
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/products.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
);

-- Create page_views table from CSV
CREATE TABLE IF NOT EXISTS page_views (
    view_id BIGINT,
    user_id INT,
    page_url STRING,
    referrer STRING,
    session_id STRING,
    device_type STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/page_views.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
);

-- Show all created tables
SHOW TABLES;
