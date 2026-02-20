WITH x AS (
  SELECT COUNT(1) AS bad_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.accounts` a
  LEFT JOIN `{{ params.project }}.{{ params.dataset }}.customers` c
    ON a.customer_id = c.customer_id
  WHERE a.customer_id IS NULL
     OR c.customer_id IS NULL
)
SELECT
  (bad_cnt = 0) AS check_pass,
  CAST(bad_cnt AS FLOAT64) AS metric_value,
  CONCAT('bad_accounts_customer_fk_cnt=', CAST(bad_cnt AS STRING)) AS details
FROM x;
