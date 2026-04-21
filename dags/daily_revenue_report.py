"""
daily_revenue_report.py
=======================
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "data-platform",
    "retries": 2,                          # Retry twice before giving up
    "retry_delay": timedelta(minutes=5),   # Wait 5 min between retries
    "email_on_failure": False,             # We handle alerting via Astro Alerts
    "email_on_retry": False,
}


@dag(
    dag_id="daily_revenue_report",
    description="Daily executive revenue summary emailed at 7 AM EST",
    # Cron: "0 12 * * *" = every day at 12:00 UTC = 7 AM EST
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
    Pipeline: extract -> calculate -> format -> send -> audit
    """
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
    # Task 2: Calculate the headline metrics for the email
    # -----------------------------------------------------------------------
    @task
    def calculate_daily_metrics(orders: list[dict]) -> dict:
        if not orders:
            return {
                "order_count": 0,
                "total_revenue": 0.0,
                "avg_order_value": 0.0,
                "top_products": [],
            }

        total_revenue = sum(float(o["order_total_usd"]) for o in orders)
        order_count = len(orders)
        aov = total_revenue / order_count
        product_revenue: dict[str, float] = {}
        for o in orders:
            name = o["product_name"]
            product_revenue[name] = product_revenue.get(name, 0.0) + float(o["order_total_usd"])
        top_products = sorted(
            product_revenue.items(), key=lambda x: x[1], reverse=True
        )[:5]

        metrics = {
            "order_count": order_count,
            "total_revenue": round(total_revenue, 2),
            "avg_order_value": round(aov, 2),
            "top_products": [{"name": n, "revenue": round(r, 2)} for n, r in top_products],
        }
        print(f"Computed metrics: {metrics}")
        return metrics

    # -----------------------------------------------------------------------
    # Task 3: Format the metrics into an HTML email body
    # -----------------------------------------------------------------------
    @task
    def format_report(metrics: dict, logical_date=None) -> str:
        target_date = (logical_date - timedelta(days=1)).strftime("%A, %B %d, %Y")

        # Build the top-products table rows.
        rows = "".join(
            f"<tr><td>{p['name']}</td><td>${p['revenue']:,.2f}</td></tr>"
            for p in metrics["top_products"]
        ) or "<tr><td colspan='2'>No orders yesterday</td></tr>"

        html = f"""
        <html><body style="font-family: Arial, sans-serif;">
          <h2>ShopSmart Daily Revenue - {target_date}</h2>
          <p><b>Total Revenue:</b> ${metrics['total_revenue']:,.2f}</p>
          <p><b>Orders:</b> {metrics['order_count']:,}</p>
          <p><b>Average Order Value:</b> ${metrics['avg_order_value']:,.2f}</p>
          <h3>Top 5 Products by Revenue</h3>
          <table border="1" cellpadding="6" cellspacing="0">
            <tr><th>Product</th><th>Revenue</th></tr>
            {rows}
          </table>
        </body></html>
        """
        return html

    # -----------------------------------------------------------------------
    # Task 4: Send the email to leadership
    # -----------------------------------------------------------------------
    @task
    def email_leadership(html_body: str, logical_date=None) -> str:

        target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        subject = f"ShopSmart Daily Revenue - {target_date}"
        recipients = ["cfo@shopsmart.example", "coo@shopsmart.example", "ceo@shopsmart.example"]
        print(f"[SIMULATED EMAIL] To: {recipients} | Subject: {subject}")
        print(f"Body preview: {html_body[:200]}...")

        return subject

    # -----------------------------------------------------------------------
    # Task 5: Write an audit record back to Snowflake
    # -----------------------------------------------------------------------
    @task
    def log_to_audit_table(email_subject: str, logical_date=None) -> None:

        target_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        sql = """
              INSERT INTO SHOPSMART.OPS.REPORT_AUDIT_LOG
                  (report_date, report_name, subject_line, sent_at_utc)
              VALUES (%s, %s, %s, CURRENT_TIMESTAMP()) \
              """
        hook.run(sql, parameters=(target_date, "daily_revenue_report", email_subject))
        print(f"Audit row written for {target_date}")
    orders = extract_orders_from_snowflake()
    metrics = calculate_daily_metrics(orders)
    report_html = format_report(metrics)
    subject = email_leadership(report_html)
    log_to_audit_table(subject)


# Instantiate the DAG. This line is what Airflow's scheduler picks up when it
# parses the file. Without it, the DAG won't register.
daily_revenue_report()
