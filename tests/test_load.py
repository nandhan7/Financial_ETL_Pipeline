import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))
from etl.load import load_data
from pyspark.sql import SparkSession
import pytest

spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()


@pytest.fixture
def sample_dataframe():
    data = [
        ("2025-07-01", "AAPL", 180.0, 182.5, 1200000),
        ("2025-07-02", "AAPL", 182.5, 185.0, 1500000)
    ]
    columns = ["stock_date", "Name", "open", "close", "volume"]
    return spark.createDataFrame(data, columns)


def test_load_data_success(sample_dataframe, tmp_path):
    # Create output path for CSV
    output_path = tmp_path / "output.csv"

    # Call the real function
    load_data(sample_dataframe, str(output_path))

    # Check the file was created and contains expected content
    assert output_path.exists()

    content = output_path.read_text()
    assert "AAPL" in content
    assert "2025-07-01" in content
