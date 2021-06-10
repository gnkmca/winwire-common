SELECT e.*,
       DATE_FORMAT(CURRENT_TIMESTAMP(), '${dtFormat}') AS dt
FROM ${extractedData} AS e