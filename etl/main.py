import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.extract import get_spark_session, extract_data
from etl.load import load_data
from etl.transform import transform_data
from dotenv import load_dotenv

load_dotenv()


venv_python = os.path.join(os.environ['VIRTUAL_ENV'], 'Scripts', 'python.exe')
os.environ['PYSPARK_PYTHON'] = venv_python
os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python


if __name__ == "__main__":
    spark = get_spark_session()

    file_path = os.getenv("input_file_path")
    df = extract_data(spark, file_path)

    transformed_df = transform_data(df)
    transformed_df.show(10)

    # Add after transformation
    output_path = os.getenv("output_file_path")

    load_data(transformed_df, output_path)

    # Next step: transform(df)
    # Later: load(transformed_df)

    spark.stop()
