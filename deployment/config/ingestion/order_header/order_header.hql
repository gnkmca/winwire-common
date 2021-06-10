create external table retail.order_header
(order_number BIGINT,quantity INT, amount INT)
STORED AS PARQUET
location "hdfs://localhost:9000/user/hive/warehouse/retail.db/order_header";
