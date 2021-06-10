create external table retail.employee
(empno BIGINT, dept_no BIGINT, elt_create_date TIMESTAMP, elt_update_date TIMESTAMP)
STORED AS PARQUET
location "hdfs://localhost:9000/user/hive/warehouse/retail.db/employee";