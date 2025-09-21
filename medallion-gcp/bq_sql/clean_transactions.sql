-- -----------------------------------------------------------------------------
-- Creates or replaces the `transactions_clean` table in the `silver` dataset.
-- 
-- This transformation performs the following:
--   - Generates a unique `id` for each transaction by hashing the combination
--     of `txnno` and `custno` from the raw data.
--   - Renames `custno` to `user_id`.
--   - Casts the `amount` field to FLOAT64 for consistency.
--   - Parses the `event_ts` string into a TIMESTAMP using the specified format.
--   - Adds an `ingest_ts` column with the current timestamp to record ingestion time.
--   - Filters out records where `amount` is NULL.
--   - Deduplicates transactions by keeping only the latest record per `txnno`
--     based on `_ingest_ts`.
-- 
-- Source: `your_project.bronze.transactions_raw`
-- Target: `your_project.silver.transactions_clean`
-- -----------------------------------------------------------------------------
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
