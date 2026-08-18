SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.raw;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;
SELECT order_id, customer_id, gross_amount, record_status
FROM iceberg.silver.sales_orders
ORDER BY order_id;
SELECT settlement_month, country, visited_operator, service_type, invoice_count, settlement_amount_usd
FROM iceberg.gold.fch_settlement_summary
ORDER BY settlement_month, country, visited_operator, service_type;
