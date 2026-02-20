WITH x AS (
  SELECT
    COUNTIF(amount <= 0) AS non_positive_amt_cnt,
    COUNTIF(currency IS NULL OR currency = '') AS missing_ccy_cnt
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
  WHERE DATE(transaction_ts) = DATE('{{ params.run_date }}')
),
y AS (
  SELECT
    (non_positive_amt_cnt + missing_ccy_cnt) AS bad_cnt,
    non_positive_amt_cnt,
    missing_ccy_cnt
  FROM x
)
SELECT
  (bad_cnt = 0) AS check_pass,
  CAST(bad_cnt AS FLOAT64) AS metric_value,
  CONCAT('bad_cnt=', CAST(bad_cnt AS STRING),
         ', non_positive_amt_cnt=', CAST(non_positive_amt_cnt AS STRING),
         ', missing_ccy_cnt=', CAST(missing_ccy_cnt AS STRING)) AS details
FROM y;
