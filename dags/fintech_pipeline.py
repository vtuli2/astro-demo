import logging
from datetime import datetime
from airflow.sdk import dag, task

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="cloud_log_validation_dag",
    schedule=None, # Manual trigger only for the demo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales_engineering', 'cloud_test']
)
def cloud_log_validation():

    @task
    def print_the_logs():
        logger.info("========================================")
        logger.info("ASTRO CLOUD DEPLOYMENT SUCCESSFUL!")
        logger.info("If you are reading this, the cloud logs are working.")
        logger.info("========================================")
        return "Success"

    print_the_logs()

cloud_log_validation()