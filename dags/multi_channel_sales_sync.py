"""
multi_channel_sales_sync.py
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "data-platform",
    "retries": 3,                           # More retries than DAG 1 because
    # third-party APIs flake constantly.
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="multi_channel_sales_sync",
    description="Hourly parallel sync of orders from Shopify, Amazon, eBay, and direct site",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["shopsmart", "ingestion", "snowflake", "parallel"],
    max_active_runs=3,
    doc_md=__doc__,
)
def multi_channel_sales_sync():


    # -----------------------------------------------------------------------
    # Four parallel channel-extraction tasks
    # -----------------------------------------------------------------------

    @task
    def sync_shopify_orders() -> dict:
        """Pull the last hour of Shopify orders via the Admin GraphQL API."""
        print("Pulling Shopify orders for the last hour...")
        fake_count = 142
        return {"channel": "shopify", "orders_synced": fake_count}
    def sync_amazon_orders() -> dict:
        """
        Pull the last hour of Amazon orders via SP-API.
        Runs on the `high_memory` worker queue - Amazon payloads include
        full line-item breakdowns, shipping history, and buyer info, which
        can be 10x the size of other channels.
        """
        print("Pulling Amazon orders (on high_memory queue)...")
        fake_count = 389
        return {"channel": "amazon", "orders_synced": fake_count}

    @task
    def sync_ebay_orders() -> dict:
        """Pull the last hour of eBay orders via the Fulfillment API."""
        print("Pulling eBay orders for the last hour...")
        fake_count = 57
        return {"channel": "ebay", "orders_synced": fake_count}

    @task
    def sync_direct_site_orders() -> dict:
        """Pull the last hour of direct-site orders from our internal orders DB."""
        print("Pulling direct-site orders for the last hour...")
        fake_count = 211
        return {"channel": "direct_site", "orders_synced": fake_count}

    # -----------------------------------------------------------------------
    # Fan-in: consolidate all four channels into one report
    # -----------------------------------------------------------------------
    @task(trigger_rule="all_done")
    def consolidate_sales_report(
            shopify: dict,
            amazon: dict,
            ebay: dict,
            direct: dict,
    ) -> dict:
        """
        Merge all four channels' results and write a consolidated row to
        Snowflake's `channel_sync_summary` table.
        """
        results = [r for r in [shopify, amazon, ebay, direct] if r is not None]
        total = sum(r["orders_synced"] for r in results)

        print(f"Consolidated {total} orders across {len(results)} channels")
        for r in results:
            print(f"  - {r['channel']}: {r['orders_synced']} orders")

        # Write the summary row
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql = """
              INSERT INTO SHOPSMART.OPS.CHANNEL_SYNC_SUMMARY
                  (sync_timestamp_utc, total_orders, channels_succeeded)
              VALUES (CURRENT_TIMESTAMP(), %s, %s) \
              """
        hook.run(sql, parameters=(total, len(results)))

        return {"total_orders": total, "channels": len(results)}

    shopify = sync_shopify_orders()
    amazon = sync_amazon_orders()
    ebay = sync_ebay_orders()
    direct = sync_direct_site_orders()

    consolidate_sales_report(shopify, amazon, ebay, direct)

multi_channel_sales_sync()
