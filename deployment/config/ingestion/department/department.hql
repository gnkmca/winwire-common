create external table retail.department
(deptno BIGINT,dept_name STRING)
STORED AS PARQUET
location "hdfs://localhost:9000/user/hive/warehouse/retail.db/department";