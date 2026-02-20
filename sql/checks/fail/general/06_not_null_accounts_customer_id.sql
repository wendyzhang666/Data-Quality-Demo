WITH x AS (
  SELECT COUNTIF(customer_id IS NULL) AS null_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.accounts`
)
SELECT
  (null_cnt = 0) AS check_pass,
  CAST(null_cnt AS FLOAT64) AS metric_value,
  CONCAT('null_accounts_customer_id_cnt=', CAST(null_cnt AS STRING)) AS details
FROM x;
