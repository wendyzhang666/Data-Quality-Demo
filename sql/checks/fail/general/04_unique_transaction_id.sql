WITH x AS (
  SELECT
    COUNT(1) AS total_cnt,
    COUNT(DISTINCT transaction_id) AS distinct_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
  WHERE DATE(transaction_ts) = DATE('{{ params.run_date }}')
),
y AS (
  SELECT
    (total_cnt - distinct_cnt) AS dup_cnt,
    total_cnt,
    distinct_cnt
  FROM x
)
SELECT
  (dup_cnt = 0) AS check_pass,
  CAST(dup_cnt AS FLOAT64) AS metric_value,
  CONCAT('dup_cnt=', CAST(dup_cnt AS STRING),
         ', total_cnt=', CAST(total_cnt AS STRING),
         ', distinct_cnt=', CAST(distinct_cnt AS STRING)) AS details
FROM y;
