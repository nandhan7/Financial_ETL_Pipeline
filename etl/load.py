import os
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import pandas as pd
from etl.logger import get_logger
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

logger = get_logger(__name__)


def load_data(df, output_path):
    # print("Pandas version:", pd.__version__)
    # df.show()
    print(type(df))

    
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.hadoop.io.native.lib.available", "false")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.parquet.output.committer.class",
                       "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    spark.conf.set("spark.sql.sources.commitProtocolClass",
                       "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

    df_pd = df.toPandas()
    
   

    # df.write.mode("overwrite") \
    #     .option("header", "true") \
    #     .option("codec", "uncompressed") \
    #     .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    #     .option("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1") \
    #     .csv(output_path)

    logger.info(f"Started Loading data to csv file")
    df_pd.to_csv(
        output_path, index=False)
    logger.info(f"Loaded data to csv file")

    df_pd.columns = [col.upper() for col in df_pd.columns]
    print(df_pd.columns)


    try:
        logger.info(f"Connecting to Snowflake to load data into table")
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df_pd,
            table_name="SP500_STOCKS",
            database="FINANCIAL_ETL",
            schema="STOCK_DATA"
        )
        logger.info("Data successfully loaded into Snowflake")
        logger.info(f"Loaded {nrows} rows to Snowflake")
        logger.info(f" Data loaded to: {output_path}")
        
        logger.info
    except Exception as e:
        logger.error(f"Loading to Snowflake failed: {e}", exc_info=True)
        raise