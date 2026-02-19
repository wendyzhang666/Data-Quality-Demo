from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator

DEFAULT_PARAMS = {
    "project": "YOUR_GCP_PROJECT",
    "dataset": "YOUR_BQ_DATASET",
    "run_date": "{{ ds }}",
    "results_table": "dq_results",
}

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"

def read_sql(rel: str) -> str:
    return (SQL_DIR / rel).read_text(encoding="utf-8")

INSERT_WRAPPER = read_sql("wrappers/insert_result.sql")

CHECKS = [
    # ---------- FAIL: General ----------
    ("FAIL", "general__freshness_transactions", "checks/fail/general/01_freshness_transactions.sql", "freshness_ok",),
    ("FAIL", "general__rowcount_today_transactions", "checks/fail/general/02_rowcount_today_transactions.sql", "today_rowcount",),
    ("FAIL", "general__not_null_transactions_keys", "checks/fail/general/03_not_null_transactions_keys.sql", "null_key_fields_cnt",),
    ("FAIL", "general__unique_transaction_id", "checks/fail/general/04_unique_transaction_id.sql", "dup_transaction_id_cnt",),
    ("FAIL", "general__fk_transactions_to_accounts", "checks/fail/general/05_fk_transactions_to_accounts.sql", "orphan_txn_account_cnt",),
    ("FAIL", "general__not_null_accounts_customer_id", "checks/fail/general/06_not_null_accounts_customer_id.sql", "null_account_customer_cnt",),
    ("FAIL", "general__fk_accounts_to_customers", "checks/fail/general/07_fk_accounts_to_customers.sql", "orphan_account_customer_cnt",),

    # ---------- FAIL: AML ----------
    ("FAIL", "aml__sanctions_freshness", "checks/fail/aml/01_sanctions_freshness.sql", "sanctions_fresh_ok",),
    ("FAIL", "aml__transaction_amount_currency_hard_rules", "checks/fail/aml/02_transaction_amount_currency_hard_rules.sql", "hard_rule_bad_cnt",),
    ("FAIL", "aml__alert_must_link_transaction", "checks/fail/aml/03_alert_must_link_transaction.sql", "orphan_alert_txn_cnt",),
    ("FAIL", "aml__case_closed_must_have_reason", "checks/fail/aml/04_case_closed_must_have_reason.sql", "closed_missing_reason_cnt",),

    # ---------- WARN: General ----------
    ("WARN", "general__rowcount_anomaly_7d", "checks/warn/general/01_rowcount_anomaly_7d.sql", "deviation_ratio",),
    ("WARN", "general__alert_volume_anomaly_7d", "checks/warn/general/02_alert_volume_anomaly_7d.sql", "alert_deviation_ratio",),

    # ---------- WARN: AML ----------
    ("WARN", "aml__kyc_missing_rate", "checks/warn/aml/01_kyc_missing_rate.sql", "kyc_missing_rate",),
    ("WARN", "aml__sanctions_fields_coverage", "checks/warn/aml/02_sanctions_fields_coverage.sql", "sanctions_bad_rate",),
    ("WARN", "aml__transaction_p99_sanity", "checks/warn/aml/03_transaction_p99_sanity.sql", "p99_amt",),
]

def wrap_insert(check_name: str, severity: str, metric_name: str, check_sql: str) -> str:
    return (
        INSERT_WRAPPER
        .replace("{{CHECK_NAME}}", check_name)
        .replace("{{SEVERITY}}", severity)
        .replace("{{METRIC_NAME}}", metric_name)
        .replace("{{CHECK_SQL}}", check_sql)
    )

with DAG(
    dag_id="aml_dq_bq_allchecks_summaryfail",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},
    params=DEFAULT_PARAMS,
    tags=["dq", "aml", "bigquery", "sql-first", "summary-fail"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1) Run ALL checks (parallel) -> write dq_results
    check_tasks = []
    for severity, check_name, sql_path, metric_name in CHECKS:
        check_sql = read_sql(sql_path)
        insert_sql = wrap_insert(check_name, severity, metric_name, check_sql)

        t = BigQueryInsertJobOperator(
            task_id=f"{severity}__{check_name}",
            configuration={"query": {"query": insert_sql, "useLegacySql": False}},
            gcp_conn_id="google_cloud_default",
        )
        start >> t
        check_tasks.append(t)

    # 2) Gatekeeper: run after ALL check tasks finish (even if some checks wrote check_pass=false)
    gatekeeper = BigQueryCheckOperator(
        task_id="GATEKEEPER__no_failures",
        sql=read_sql("summary/gatekeeper_no_failures.sql"),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        trigger_rule="all_done",
    )

    for t in check_tasks:
        t >> gatekeeper

    gatekeeper >> end
