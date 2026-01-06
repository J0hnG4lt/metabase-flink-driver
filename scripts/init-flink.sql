-- ============================================
-- Flink SQL Initialization Script
-- Creates sample tables for testing Metabase
-- ============================================

-- Create a datagen source table for users
CREATE TABLE IF NOT EXISTS users (
    user_id INT,
    username STRING,
    email STRING,
    created_at TIMESTAMP(3),
    age INT,
    country STRING,
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '1',
    'fields.user_id.end' = '1000000',
    'fields.username.length' = '10',
    'fields.email.length' = '15',
    'fields.age.min' = '18',
    'fields.age.max' = '80',
    'fields.country.length' = '5'
);

-- Create a datagen source table for orders
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10, 2),
    order_time TIMESTAMP(3),
    status STRING,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '10000000',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '1000',
    'fields.product_name.length' = '12',
    'fields.quantity.min' = '1',
    'fields.quantity.max' = '10',
    'fields.unit_price.min' = '1',
    'fields.unit_price.max' = '500',
    'fields.status.length' = '8'
);

-- Create a datagen source table for products
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    last_updated TIMESTAMP(3),
    WATERMARK FOR last_updated AS last_updated - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.product_id.kind' = 'sequence',
    'fields.product_id.start' = '1',
    'fields.product_id.end' = '100000',
    'fields.product_name.length' = '15',
    'fields.category.length' = '8',
    'fields.price.min' = '5',
    'fields.price.max' = '1000',
    'fields.stock_quantity.min' = '0',
    'fields.stock_quantity.max' = '500'
);

-- Create a datagen source table for page views (for analytics)
CREATE TABLE IF NOT EXISTS page_views (
    view_id BIGINT,
    user_id INT,
    page_url STRING,
    referrer STRING,
    view_time TIMESTAMP(3),
    session_id STRING,
    device_type STRING,
    WATERMARK FOR view_time AS view_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.view_id.kind' = 'sequence',
    'fields.view_id.start' = '1',
    'fields.view_id.end' = '100000000',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '10000',
    'fields.page_url.length' = '20',
    'fields.referrer.length' = '15',
    'fields.session_id.length' = '32',
    'fields.device_type.length' = '6'
);

-- Show all created tables
SHOW TABLES;
