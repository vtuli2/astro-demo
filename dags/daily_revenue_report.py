"""
daily_revenue_report.py
=======================
"""

from __future__ import annotations
from datetime import datetime, timedelta
from collections import defaultdict
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="daily_revenue_report",
    description="Daily executive revenue summary logged to console",
    schedule="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["shopsmart", "reporting", "executive", "snowflake"],
    max_active_runs=1,
    doc_md=__doc__,
)
def daily_revenue_report():
    """
    Pipeline: extract -> calculate -> log to console -> audit
    """

    # -----------------------------------------------------------------------
    # Task 1: Extract (Unchanged)
    # -----------------------------------------------------------------------
    @task
    def extract_orders_from_snowflake(logical_date=None) -> list[dict]:
        target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql = """
              SELECT
                  order_id,
                  customer_id,
                  product_id,
                  product_name,
                  order_total_usd,
                  order_timestamp
              FROM SHOPSMART.PROD.ORDERS
              WHERE DATE(order_timestamp) = %s
                AND order_status = 'COMPLETED' \
              """

        records = hook.get_records(sql, parameters=(target_date,))
        columns = [
            "order_id", "customer_id", "product_id",
            "product_name", "order_total_usd", "order_timestamp",
        ]
        orders = [dict(zip(columns, row)) for row in records]

        print(f"Extracted {len(orders)} completed orders for {target_date}")
        return orders

    # -----------------------------------------------------------------------
    # Task 2: Calculate the headline metrics (Simplified)
    # -----------------------------------------------------------------------
    @task
    def calculate_daily_metrics(orders: list[dict]) -> dict:
        if not orders:
            return {"order_count": 0, "total_revenue": 0.0, "avg_order_value": 0.0, "top_products": []}

        # Calculate totals
        revenues = [float(o["order_total_usd"]) for o in orders]
        total_revenue = sum(revenues)

        # Aggregate product revenue using defaultdict for cleaner code
        product_revenue = defaultdict(float)
        for o in orders:
            product_revenue[o["product_name"]] += float(o["order_total_usd"])

        # Sort and get top 5
        top_products = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:5]

        metrics = {
            "order_count": len(orders),
            "total_revenue": round(total_revenue, 2),
            "avg_order_value": round(total_revenue / len(orders), 2),
            "top_products": [{"name": n, "revenue": round(r, 2)} for n, r in top_products],
        }
        return metrics

    # -----------------------------------------------------------------------
    # Task 3: Log metrics to console in a UI-friendly format
    # -----------------------------------------------------------------------
    @task
    def log_console_report(metrics: dict, logical_date=None) -> str:
        target_date = (logical_date - timedelta(days=1)).strftime("%A, %B %d, %Y")

        print("=" * 60)
        print(f"DAILY REVENUE REPORT: {target_date} ")
        print("=" * 60)
        print(f" Total Revenue:       ${metrics['total_revenue']:,.2f}")
        print(f" Total Orders:        {metrics['order_count']:,}")
        print(f" Average Order Value: ${metrics['avg_order_value']:,.2f}")
        print("-" * 60)
        print("TOP 5 PRODUCTS")
        print("-" * 60)

        if not metrics["top_products"]:
            print(" No products sold yesterday.")
        else:
            for i, p in enumerate(metrics["top_products"], 1):
                # .ljust(40) ensures the dollar signs align neatly
                print(f" {i}. {p['name'].ljust(40)} ${p['revenue']:,.2f}")
        print("=" * 60)

        return f"Console report logged for {target_date}"

    # -----------------------------------------------------------------------
    # Task 4: Write an audit record back to Snowflake
    # -----------------------------------------------------------------------
    @task
    def log_to_audit_table(status_message: str, logical_date=None) -> None:
        target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        sql = """
              INSERT INTO SHOPSMART.OPS.REPORT_AUDIT_LOG
                  (report_date, report_name, subject_line, sent_at_utc)
              VALUES (%s, %s, %s, CURRENT_TIMESTAMP()) \
              """
        # We pass the status_message into the subject_line column since email is removed
        hook.run(sql, parameters=(target_date, "daily_revenue_report_console", status_message))
        print(f"Audit row written for {target_date}")

    # -----------------------------------------------------------------------
    # DAG Dependencies
    # -----------------------------------------------------------------------
    orders = extract_orders_from_snowflake()
    metrics = calculate_daily_metrics(orders)
    status_msg = log_console_report(metrics)
    log_to_audit_table(status_msg)

daily_revenue_report()