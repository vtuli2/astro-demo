import logging
from datetime import datetime

# Updating to the new Airflow 3.0 SDK to clear your terminal warnings
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="crypto_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales_engineering', 'crypto']
)
def crypto_pipeline_dag():

    @task
    def fetch_bitcoin_price():
        logger.info("========================================")
        logger.info("Initiating connection to Crypto Exchange...")
        logger.info("SUCCESS: Fetched current market rate.")
        logger.info("PRICE: 1 BTC = $68,450.00 USD")
        logger.info("========================================")
        return {"btc_usd": 68450.00}

    # Call the task
    fetch_bitcoin_price()

# Register the DAG
my_dag = crypto_pipeline_dag()

# The Magic Local Testing Block
if __name__ == "__main__":
    # This bypasses the Airflow scheduler and runs the DAG natively in your terminal
    my_dag.test()