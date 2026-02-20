WITH x AS (
  SELECT COUNT(1) AS bad_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.cases`
  WHERE status = 'closed'
    AND (closing_reason IS NULL OR closing_reason = '')
)
SELECT
  (bad_cnt = 0) AS check_pass,
  CAST(bad_cnt AS FLOAT64) AS metric_value,
  CONCAT('closed_missing_reason_cnt=', CAST(bad_cnt AS STRING)) AS details
FROM x;
