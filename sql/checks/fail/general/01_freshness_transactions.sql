WITH x AS (
  SELECT MAX(DATE(transaction_ts)) AS max_dt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
)
SELECT
  (max_dt >= DATE('{{ params.run_date }}')) AS check_pass,
  CAST(DATE_DIFF(DATE('{{ params.run_date }}'), max_dt, DAY) AS FLOAT64) AS metric_value,
  CONCAT('max_dt=', CAST(max_dt AS STRING),
         ', run_date=', CAST(DATE('{{ params.run_date }}') AS STRING),
         ', lag_days=', CAST(DATE_DIFF(DATE('{{ params.run_date }}'), max_dt, DAY) AS STRING)) AS details
FROM x;
