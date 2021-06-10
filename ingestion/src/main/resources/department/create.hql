CREATE TABLE IF NOT EXISTS ${db}.${table}
(
    `deptno`    BIGINT,
    `dept_name`  STRING
)
    USING ${format}
    LOCATION '${path}'
