-- Drop schema and tables and views if needed
--DROP VIEW dp700_e016.vw_customers;
--DROP VIEW dp700_e016.vw_products;
--DROP VIEW dp700_e016.vw_sales_summary;
--DROP TABLE dp700_e016.customer;
--DROP TABLE dp700_e016.product;
--DROP TABLE dp700_e016.sales;
--DROP SCHEMA dp700_e016;

-- Create schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dp700_e016')
BEGIN
    EXEC('CREATE SCHEMA dp700_e016');
END;
GO

-- Create customer table
CREATE TABLE dp700_e016.customer (
    customer_id INT,
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    email NVARCHAR(100),
    created_at DATETIME
);
GO

-- Create product table
CREATE TABLE dp700_e016.product (
    product_id INT,
    product_name NVARCHAR(100),
    category NVARCHAR(50),
    price DECIMAL(10, 2),
    is_active BIT
);
GO

-- Create sales table
CREATE TABLE dp700_e016.sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    sale_date DATE
);
GO

-- Create view for customers
CREATE VIEW dp700_e016.vw_customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM dp700_e016.customer;
GO

-- Create view for products
CREATE VIEW dp700_e016.vw_products AS
SELECT
    product_id,
    product_name,
    category,
    price,
    is_active
FROM dp700_e016.product;
GO

-- Create a simple sales summary view
CREATE VIEW dp700_e016.vw_sales_summary AS
SELECT
    s.sale_id,
    s.customer_id,
    s.product_id,
    p.product_name,
    c.first_name + ' ' + c.last_name AS customer_name,
    s.quantity,
    s.sale_date,
    (s.quantity * p.price) AS total_value
FROM dp700_e016.sales s
JOIN dp700_e016.customer c ON s.customer_id = c.customer_id
JOIN dp700_e016.product p ON s.product_id = p.product_id;
GO


-- Insert into customer table
INSERT INTO dp700_e016.customer (customer_id, first_name, last_name, email, created_at)
VALUES
(1, 'Alice', 'Johnson', 'alice.johnson@example.com', '2024-01-10'),
(2, 'Bob', 'Smith', 'bob.smith@example.com', '2024-02-15'),
(3, 'Charlie', 'Lee', 'charlie.lee@example.com', '2024-03-20');
GO

-- Insert into product table
INSERT INTO dp700_e016.product (product_id, product_name, category, price, is_active)
VALUES
(101, 'Laptop Pro 14"', 'Electronics', 1299.99, 1),
(102, 'Office Chair', 'Furniture', 199.50, 1),
(103, 'Wireless Mouse', 'Accessories', 29.99, 1);
GO

-- Insert into sales table
INSERT INTO dp700_e016.sales (sale_id, customer_id, product_id, quantity, sale_date)
VALUES
(1001, 1, 101, 1, '2024-04-01'),
(1002, 2, 102, 2, '2024-04-02'),
(1003, 3, 103, 3, '2024-04-03'),
(1004, 1, 103, 1, '2024-04-04');
GO

/*********************************************************************************************************/

-- Add a new customer
INSERT INTO dp700_e016.customer (customer_id, first_name, last_name, email, created_at)
VALUES (4, 'Diana', 'Nguyen', 'diana.nguyen@example.com', '2024-05-10');
GO

-- Add a new sale for Diana
INSERT INTO dp700_e016.sales (sale_id, customer_id, product_id, quantity, sale_date)
VALUES (1005, 4, 102, 1, '2024-05-10');
GO


-- Update customer email
UPDATE dp700_e016.customer
SET email = 'alice.johnson@newdomain.com'
WHERE customer_id = 1;
GO


-- Delete a sale
DELETE FROM dp700_e016.sales
WHERE sale_id = 1004;
GO

/*********************************************************************************************************/

-- Drop table if needed
-- DROP TABLE dp700_e016.support_ticket;

-- Create support_ticket table
CREATE TABLE dp700_e016.support_ticket (
    ticket_id INT,
    customer_id INT,
    subject NVARCHAR(100),
    status NVARCHAR(20),
    created_at DATETIME
);
GO

INSERT INTO dp700_e016.support_ticket (ticket_id, customer_id, subject, status, created_at)
VALUES
(501, 1, 'Laptop not working', 'Open', '2024-04-05'),
(502, 2, 'Chair arrived damaged', 'Closed', '2024-04-07'),
(503, 3, 'Mouse missing from order', 'Open', '2024-04-08');
GO