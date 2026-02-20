WITH daily AS (
  SELECT DATE(transaction_ts) AS dt, COUNT(1) AS cnt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
  WHERE DATE(transaction_ts) BETWEEN DATE_SUB(DATE('{{ params.run_date }}'), INTERVAL 7 DAY)
                                AND DATE('{{ params.run_date }}')
  GROUP BY 1
),
today AS (
  SELECT COALESCE(cnt, 0) AS today_cnt
  FROM daily
  WHERE dt = DATE('{{ params.run_date }}')
),
hist AS (
  SELECT COALESCE(AVG(cnt), 0) AS avg_7d
  FROM daily
  WHERE dt BETWEEN DATE_SUB(DATE('{{ params.run_date }}'), INTERVAL 7 DAY)
              AND DATE_SUB(DATE('{{ params.run_date }}'), INTERVAL 1 DAY)
),
calc AS (
  SELECT
    today_cnt,
    avg_7d,
    ABS(today_cnt - avg_7d) / NULLIF(avg_7d, 0) AS deviation_ratio
  FROM today CROSS JOIN hist
)
SELECT
  (COALESCE(deviation_ratio, 0) <= 0.30) AS check_pass,
  CAST(COALESCE(deviation_ratio, 0) AS FLOAT64) AS metric_value,
  CONCAT('deviation_ratio=', CAST(COALESCE(deviation_ratio, 0) AS STRING),
         ', today_cnt=', CAST(today_cnt AS STRING),
         ', avg_7d=', CAST(avg_7d AS STRING),
         ', threshold=0.30') AS details
FROM calc;
