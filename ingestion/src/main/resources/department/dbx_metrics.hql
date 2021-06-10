SELECT '${targetTable}_count' AS metric,
       COUNT(*)               AS value
FROM ${targetDb}.${targetTable}
UNION ALL
SELECT '${targetTable}_last_loaded_date'                AS metric,
       NVL(UNIX_TIMESTAMP(MAX(${updateDateColumn})), 0) AS value
FROM ${targetDb}.${targetTable}
UNION ALL
SELECT '${targetTable}_max_dt'                        AS metric,
       NVL(UNIX_TIMESTAMP(MAX(dt), '${dtFormat}'), 0) AS value
FROM ${targetDb}.${targetTable}