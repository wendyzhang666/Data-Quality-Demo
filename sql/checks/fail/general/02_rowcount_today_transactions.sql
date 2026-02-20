WITH x AS (
  SELECT COUNT(1) AS today_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
  WHERE DATE(transaction_ts) = DATE('{{ params.run_date }}')
)
SELECT
  (today_cnt > 0) AS check_pass,
  CAST(today_cnt AS FLOAT64) AS metric_value,
  CONCAT('today_cnt=', CAST(today_cnt AS STRING),
         ', run_date=', CAST(DATE('{{ params.run_date }}') AS STRING)) AS details
FROM x;
