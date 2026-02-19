from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

DEFAULT_PARAMS = {
    "project": "YOUR_GCP_PROJECT",
    "dataset": "YOUR_BQ_DATASET",
    "run_date": "{{ ds }}",  # YYYY-MM-DD
    "results_table": "dq_results",
}

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"

FAIL_GENERAL = [
    "fail/general/01_freshness_transactions.sql",
    "fail/general/02_rowcount_today_transactions.sql",
    "fail/general/03_not_null_transactions_keys.sql",
    "fail/general/04_unique_transaction_id.sql",
    "fail/general/05_fk_transactions_to_accounts.sql",
    "fail/general/06_not_null_accounts_customer_id.sql",
    "fail/general/07_fk_accounts_to_customers.sql",
]

FAIL_AML = [
    "fail/aml/01_sanctions_freshness.sql",
    "fail/aml/02_transaction_amount_currency_hard_rules.sql",
    "fail/aml/03_alert_must_link_transaction.sql",
    "fail/aml/04_case_closed_must_have_reason.sql",
]

WARN_GENERAL = [
    "warn/general/01_rowcount_anomaly_7d.sql",
    "warn/general/02_alert_volume_anomaly_7d.sql",
]

WARN_AML = [
    "warn/aml/01_kyc_missing_rate.sql",
    "warn/aml/02_sanctions_fields_coverage.sql",
    "warn/aml/03_transaction_p99_sanity.sql",
]

def read_sql(rel: str) -> str:
    return (SQL_DIR / rel).read_text(encoding="utf-8")

WARN_WRAPPER = read_sql("warn_wrappers/insert_warn_result.sql")

def make_warn_insert_sql(check_name: str, metric_name: str, check_sql: str) -> str:
    """
    Wrap a WARN check SQL (must return: check_pass BOOL, metric_value FLOAT64, details STRING)
    into an INSERT INTO dq_results statement.
    """
    return WARN_WRAPPER.replace("{{CHECK_NAME}}", check_name) \
                      .replace("{{METRIC_NAME}}", metric_name) \
                      .replace("{{CHECK_SQL}}", check_sql)

with DAG(
    dag_id="aml_dq_bigquery_fail_warn",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},  # 练习：DQ 逻辑失败通常重试没意义
    params=DEFAULT_PARAMS,
    tags=["dq", "aml", "bigquery", "fail-warn", "sql-first"],
) as dag:
    start = EmptyOperator(task_id="start")
    done_fail = EmptyOperator(task_id="done_fail_branch")
    done_warn = EmptyOperator(task_id="done_warn_branch")
    end = EmptyOperator(task_id="end")

    # --------------------------
    # FAIL branch (blocking)
    # --------------------------
    prev_fail = start
    for i, rel in enumerate(FAIL_GENERAL, start=1):
        t = BigQueryCheckOperator(
            task_id=f"FAIL_general_{i:02d}_{Path(rel).stem}",
            sql=read_sql(rel),
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
        )
        prev_fail >> t
        prev_fail = t

    for i, rel in enumerate(FAIL_AML, start=1):
        t = BigQueryCheckOperator(
            task_id=f"FAIL_aml_{i:02d}_{Path(rel).stem}",
            sql=read_sql(rel),
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
        )
        prev_fail >> t
        prev_fail = t

    prev_fail >> done_fail

    # --------------------------
    # WARN branch (non-blocking)
    # - writes results into dq_results
    # - must run even if FAIL branch fails
    # --------------------------
    prev_warn = start

    # General WARN
    for i, rel in enumerate(WARN_GENERAL, start=1):
        check_sql = read_sql(rel)
        check_name = f"general__{Path(rel).stem}"
        metric_name = "metric_value"

        insert_sql = make_warn_insert_sql(check_name, metric_name, check_sql)

        t = BigQueryInsertJobOperator(
            task_id=f"WARN_general_{i:02d}_{Path(rel).stem}",
            configuration={
                "query": {
                    "query": insert_sql,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="google_cloud_default",
            # 关键：让 WARN 分支不被 FAIL 分支影响（即使 FAIL 失败，也继续跑）
            trigger_rule="all_done",
        )
        prev_warn >> t
        prev_warn = t

    # AML WARN
    for i, rel in enumerate(WARN_AML, start=1):
        check_sql = read_sql(rel)
        check_name = f"aml__{Path(rel).stem}"
        metric_name = "metric_value"

        insert_sql = make_warn_insert_sql(check_name, metric_name, check_sql)

        t = BigQueryInsertJobOperator(
            task_id=f"WARN_aml_{i:02d}_{Path(rel).stem}",
            configuration={
                "query": {
                    "query": insert_sql,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="google_cloud_default",
            trigger_rule="all_done",
        )
        prev_warn >> t
        prev_warn = t

    prev_warn >> done_warn

    # End
    # end 依赖两个分支都结束；即使 fail branch 某个 task failed，这里 end 也会显示 upstream_failed
    done_fail >> end
    done_warn >> end
