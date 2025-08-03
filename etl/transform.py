from pyspark.sql.functions import col, to_date, round, expr, avg
from pyspark.sql.window import Window
from etl.logger import get_logger

logger = get_logger(__name__)

def transform_data(df):
    try:
        logger.info("Starting transformation process")

        # 1. Drop rows with nulls in important columns
        df_clean = df.dropna(subset=["open", "close", "volume", "date", "Name"])
        logger.info(f"After dropping nulls: {df_clean.count()} rows")

        # 2. Convert 'date' column to DateType
        df_clean = df_clean.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        # 3. Calculate daily_change = close - open
        df_clean = df_clean.withColumn("daily_change", round(col("close") - col("open"), 2))

        # 4. Filter where volume > 1,000,000
        df_filtered = df_clean.filter(col("volume") > 1000000)
        logger.info(f"After volume filter: {df_filtered.count()} rows")

        # 5. Add 7-day rolling average of close price per stock
        window_spec = Window.partitionBy("Name").orderBy("date").rowsBetween(-6, 0)
        df_final = df_filtered.withColumn("Seven_day_avg_close", round(avg("close").over(window_spec), 2))

        # Rename date to stock_date for consistency
        df_final = df_final.withColumnRenamed("date", "stock_date")

        logger.info("[Success] Data transformed successfully")
        return df_final

    except Exception as e:
        logger.error(f"Error during transformation: {e}", exc_info=True)
        raise
