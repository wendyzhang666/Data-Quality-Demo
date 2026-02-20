WITH x AS (
  SELECT MAX(DATE(load_date)) AS max_load_dt
  FROM `{{ params.project }}.{{ params.dataset }}.sanctions_list`
),
y AS (
  SELECT
    max_load_dt,
    DATE_DIFF(DATE('{{ params.run_date }}'), max_load_dt, DAY) AS lag_days
  FROM x
)
SELECT
  (max_load_dt >= DATE_SUB(DATE('{{ params.run_date }}'), INTERVAL 2 DAY)) AS check_pass,
  CAST(lag_days AS FLOAT64) AS metric_value,
  CONCAT('max_load_dt=', CAST(max_load_dt AS STRING),
         ', lag_days=', CAST(lag_days AS STRING),
         ', allowed_lag_days<=2') AS details
FROM y;
