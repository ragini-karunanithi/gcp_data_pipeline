CREATE OR REPLACE TABLE `your_project.silver.transactions_clean` AS
SELECT
  TO_HEX(SHA256(CONCAT(COALESCE(txnno,''), COALESCE(custno,'')))) AS id,
  custno AS user_id,
  SAFE_CAST(amount AS FLOAT64) AS amount,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(event_ts)) AS event_ts,
  CURRENT_TIMESTAMP() AS ingest_ts
FROM `your_project.bronze.transactions_raw`
WHERE amount IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY txnno ORDER BY _ingest_ts DESC) = 1;
