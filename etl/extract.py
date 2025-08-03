from pyspark.sql import SparkSession
from etl.logger import get_logger
import os

logger = get_logger(__name__)

def get_spark_session(app_name="FinancialETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.hadoop.native.lib", "false") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()


def extract_data(spark, file_path):
    try:
        if not os.path.exists(file_path):
            logger.error(f"Input file not found: {file_path}")
            raise FileNotFoundError(f"Input file not found: {file_path}")

        logger.info(f"Extracting data from {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        logger.info("[Success] Data extracted successfully")
        logger.info(f"Schema:\n{df.printSchema()}")
        logger.info(f"Row count: {df.count()} | Column count: {len(df.columns)}")

        return df

    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        raise
