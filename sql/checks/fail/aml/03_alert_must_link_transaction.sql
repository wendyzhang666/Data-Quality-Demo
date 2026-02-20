WITH x AS (
  SELECT COUNT(1) AS orphan_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.alerts` al
  LEFT JOIN `{{ params.project }}.{{ params.dataset }}.transactions` t
    ON al.transaction_id = t.transaction_id
  WHERE DATE(al.alert_ts) = DATE('{{ params.run_date }}')
    AND al.transaction_id IS NOT NULL
    AND t.transaction_id IS NULL
)
SELECT
  (orphan_cnt = 0) AS check_pass,
  CAST(orphan_cnt AS FLOAT64) AS metric_value,
  CONCAT('orphan_alert_txn_cnt=', CAST(orphan_cnt AS STRING)) AS details
FROM x;
