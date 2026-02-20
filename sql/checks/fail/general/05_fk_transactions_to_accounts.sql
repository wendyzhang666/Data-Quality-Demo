WITH x AS (
  SELECT COUNT(1) AS orphan_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions` t
  LEFT JOIN `{{ params.project }}.{{ params.dataset }}.accounts` a
    ON t.account_id = a.account_id
  WHERE DATE(t.transaction_ts) = DATE('{{ params.run_date }}')
    AND a.account_id IS NULL
)
SELECT
  (orphan_cnt = 0) AS check_pass,
  CAST(orphan_cnt AS FLOAT64) AS metric_value,
  CONCAT('orphan_txn_account_cnt=', CAST(orphan_cnt AS STRING)) AS details
FROM x;
