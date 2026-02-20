WITH x AS (
  SELECT
    COUNTIF(transaction_id IS NULL) AS null_transaction_id,
    COUNTIF(account_id IS NULL) AS null_account_id,
    COUNTIF(customer_id IS NULL) AS null_customer_id,
    COUNTIF(amount IS NULL) AS null_amount,
    COUNTIF(currency IS NULL OR currency = '') AS null_currency,
    COUNTIF(transaction_ts IS NULL) AS null_transaction_ts
  FROM `{{ params.project }}.{{ params.dataset }}.transactions`
  WHERE DATE(transaction_ts) = DATE('{{ params.run_date }}')
),
y AS (
  SELECT
    (null_transaction_id + null_account_id + null_customer_id + null_amount + null_currency + null_transaction_ts) AS bad_cnt,
    null_transaction_id, null_account_id, null_customer_id, null_amount, null_currency, null_transaction_ts
  FROM x
)
SELECT
  (bad_cnt = 0) AS check_pass,
  CAST(bad_cnt AS FLOAT64) AS metric_value,
  CONCAT(
    'bad_cnt=', CAST(bad_cnt AS STRING),
    ', null_transaction_id=', CAST(null_transaction_id AS STRING),
    ', null_account_id=', CAST(null_account_id AS STRING),
    ', null_customer_id=', CAST(null_customer_id AS STRING),
    ', null_amount=', CAST(null_amount AS STRING),
    ', null_currency=', CAST(null_currency AS STRING),
    ', null_transaction_ts=', CAST(null_transaction_ts AS STRING)
  ) AS details
FROM y;
