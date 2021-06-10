CREATE TABLE IF NOT EXISTS ${db}.${table}
(
    `order_number`    BIGINT,
    `quantity`           INT,
    `amount`    INT
)
    USING ${format}
    LOCATION '${path}'