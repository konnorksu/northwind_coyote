CREATE OR REPLACE DATABASE northwind_coyote;
CREATE OR REPLACE STAGE coyote_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE products_staging (
    ProductId INT,
    ProductName STRING,
    SupplierId INT,
    CategoryId INT,
    Unit STRING,
    Price INT
);

CREATE OR REPLACE TABLE orders_staging (
    OrderId INT,
    CustomerId INT,
    EmployeeId INT,
    OrderDate STRING,
    ShipperId INT
);

CREATE OR REPLACE TABLE customers_staging (
    CustomerId INT,
    CustomerName STRING,
    ContactName STRING,
    Address STRING,
    City STRING,
    PostalCode STRING,
    Country STRING
);

CREATE OR REPLACE TABLE categories_staging (
    CategoryId INT,
    CategoryName STRING,
    Description STRING
);

CREATE OR REPLACE TABLE employees_staging (
    EmployeeId INT,
    LastName STRING,
    FirstName STRING,
    BirthDate STRING,
    Photo STRING,
    Notes STRING
);

CREATE OR REPLACE TABLE orderdetails_staging (
    OrderDeteilId INT,
    OrderId STRING,
    ProductId STRING,
    Quantity INT
);

CREATE OR REPLACE TABLE shippers_staging (
    ShipperId INT,
    ShipperName STRING,
    Phone STRING
);

CREATE OR REPLACE TABLE suppliers_staging (
    SupplierId INT,
    SupplierName STRING,
    ContactName STRING,
    Address STRING,
    City STRING,
    PostalCode STRING,
    Country STRING,
    Phone STRING  
);

COPY INTO products_staging
FROM @coyote_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orders_staging
FROM @coyote_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customers_staging
FROM @coyote_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO categories_staging
FROM @coyote_stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging
FROM @coyote_stage/employees.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orderdetails_staging
FROM @coyote_stage/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO shippers_staging
FROM @coyote_stage/shippers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO suppliers_staging
FROM @coyote_stage/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT * FROM products_staging;
SELECT * FROM orders_staging;
SELECT * FROM customers_staging;
SELECT * FROM categories_staging;
SELECT * FROM employees_staging;
SELECT * FROM orderdetails_staging;
SELECT * FROM shippers_staging;
SELECT * FROM suppliers_staging;

CREATE OR REPLACE TABLE dim_customers AS 
SELECT
    CustomerId as CustomerId,
    CustomerName as CustomerName,
    address as address,
    city as city,
    country as country,
    PostalCode as PostalCode
FROM customers_staging;

SELECT * FROM dim_customers;

CREATE OR REPLACE TABLE dim_suppliers AS 
SELECT
    SupplierId as SupplierId,
    SupplierName as SupplierName,
    ContactName as ContactName,
    address as address,
    city as city,
    country as country,
    PostalCode as PostalCode
FROM suppliers_staging;

SELECT * FROM dim_suppliers;

CREATE OR REPLACE TABLE dim_employees AS
SELECT 
    EmployeeId as employeeId,
    FirstName as FirstName,
    LastName as LastName,
    BirthDate as BirthDate
FROM employees_staging;

SELECT * FROM dim_employees;

CREATE OR REPLACE TABLE dim_products AS
SELECT 
    p.ProductId AS productId,
    p.productName,
    p.price,
    c.categoryName,
FROM products_staging p
JOIN categories_staging c ON p.categoryId = c.CategoryId;

SELECT * FROM dim_products;

CREATE OR REPLACE TABLE bridge AS
SELECT 
    OrderDeteilId,
    orderId,
    productId
FROM orderdetails_staging;

SELECT * FROM bridge;


CREATE OR REPLACE TABLE dim_shippers AS
SELECT 
    ShipperId,
    ShipperName,
    Phone
FROM shippers_staging;

SELECT * FROM dim_shippers;

CREATE OR REPLACE TABLE dim_date AS
SELECT
    DISTINCT
    DATE_PART('year', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS day,
FROM orders_staging;

SELECT * FROM dim_date;


CREATE OR REPLACE TABLE fact_orders AS
SELECT
    o.OrderId AS orderId,
    od.quantity AS Quantity,
    ps.price AS unitPrice,
    (od.quantity * ps.price) AS totalPrice,
    b.OrderDeteilId AS bridgeId,
    TO_DATE(TO_TIMESTAMP(o.orderDate, 'YYYY-MM-DD HH24:MI:SS')) AS DateOfOrder,
    e.employeeId,
    ps.supplierId AS supplierId,
    c.customerId AS customerId
FROM orders_staging o
JOIN bridge b ON o.orderId = b.orderId
JOIN orderdetails_staging od ON b.orderId = od.orderid AND b.productId = od.productId
JOIN products_staging ps ON b.productId = ps.productId
JOIN dim_employees e ON o.employeeId = e.employeeId
JOIN dim_customers c ON o.customerId = c.customerId;

SELECT * FROM fact_orders;

DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS orderdetails_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;

SELECT 
    DATE_PART('year', DateOfOrder) AS year, 
    SUM(totalPrice) AS total_sales
FROM fact_orders
GROUP BY year
ORDER BY year;

SELECT 
    dc.CustomerName, 
    SUM(fo.totalPrice) AS total_purchases
FROM fact_orders fo
JOIN dim_customers dc ON fo.customerId = dc.customerId
GROUP BY dc.CustomerName
ORDER BY total_purchases DESC
LIMIT 5;

SELECT 
    de.FirstName || ' ' || de.LastName AS employee_name, 
    COUNT(fo.orderId) AS total_orders
FROM fact_orders fo
JOIN dim_employees de ON fo.employeeId = de.employeeId
GROUP BY employee_name
ORDER BY total_orders DESC
LIMIT 5;

SELECT 
    ds.SupplierName, 
    SUM(fo.totalPrice) AS total_sales
FROM fact_orders fo
JOIN dim_suppliers ds ON fo.supplierId = ds.SupplierId
GROUP BY ds.SupplierName
ORDER BY total_sales DESC;


SELECT 
    dc.city, 
    COUNT(fo.orderId) AS total_orders
FROM fact_orders fo
JOIN dim_customers dc ON fo.customerId = dc.customerId
GROUP BY dc.city
ORDER BY total_orders DESC;

