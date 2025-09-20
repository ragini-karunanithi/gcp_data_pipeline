CREATE OR REPLACE TABLE `your_project.gold.sales_summary` AS
SELECT
  user_id,
  DATE(event_ts) AS date,
  SUM(amount) AS total_amount,
  COUNT(1) AS txn_count
FROM `your_project.silver.transactions_clean`
GROUP BY user_id, DATE(event_ts);
