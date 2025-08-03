import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest
from pyspark.sql import SparkSession
from etl.transform import transform_data


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TransformTest").getOrCreate()

@pytest.fixture
def input_dataframe(spark):
    data = [
        ("2025-07-01", "AAPL", 180.0, 182.5, 1200000),
        ("2025-07-02", "AAPL", 182.5, 185.0, 1500000),
        ("2025-07-03", "AAPL", 185.0, 183.0, 900000),  # volume < 1M, should be filtered out
        ("2025-07-01", "MSFT", 300.0, 305.0, 2000000),
        (None, "MSFT", 310.0, 315.0, 2000000),          # date is None, should be dropped
    ]
    columns = ["date", "Name", "open", "close", "volume"]
    return spark.createDataFrame(data, columns)

def test_transform_data_basic(input_dataframe):
    # Act
    result_df = transform_data(input_dataframe)

    # Assert
    result = result_df.collect()

    assert len(result) == 3  # One row filtered by volume, one by null
    for row in result:
        assert row["daily_change"] == round(row["close"] - row["open"], 2)
        assert row["Seven_day_avg_close"] is not None
        assert "stock_date" in result_df.columns
