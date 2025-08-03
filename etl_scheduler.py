import schedule
import time
import subprocess
from datetime import datetime
from etl.logger import get_logger

logger = get_logger("ETL_Scheduler")

def run_etl():
    logger.info("Starting ETL job...")
    try:
        # Run your shell script
        subprocess.run(["bash", "scripts/run_etl.sh"], check=True)
        logger.info("ETL job completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ETL job failed: {e}")

# Schedule to run every 5 minutes
schedule.every(1).minutes.do(run_etl)

logger.info("Scheduler started...")

while True:
    schedule.run_pending()
    time.sleep(60)
