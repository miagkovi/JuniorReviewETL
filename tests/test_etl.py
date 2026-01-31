import pytest
from pyspark.sql import SparkSession
from src.transformations import (
    build_pizza_sales_flat_table, 
    get_total_sales_by_date
)

@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PyTest-Spark-Session") \
        .getOrCreate()

def test_build_pizza_sales_flat_table(spark):
    """Test the denormalized table construction."""
    # Test data setup
    details = spark.createDataFrame([(1, 101, "p_1", 2)], ["order_details_id", "order_id", "pizza_id", "quantity"])
    orders = spark.createDataFrame([(101, "2015-01-01", "12:00:00")], ["order_id", "date", "time"])
    pizzas = spark.createDataFrame([("p_1", "margarita", "S", 10.0)], ["pizza_id", "pizza_type_id", "size", "price"])
    types = spark.createDataFrame([("margarita", "Margarita", "Classic", "Cheese")], ["pizza_type_id", "name", "category", "ingredients"])

    # Call the function to test
    result_df = build_pizza_sales_flat_table(details, orders, pizzas, types)

    # Assertions
    assert result_df.count() == 1
    assert "category" in result_df.columns
    assert result_df.collect()[0]["category"] == "Classic"

def test_get_total_sales_by_date(spark):
    """Test the logic of counting pizzas."""
    # Data setup
    data = [
        ("cali_ckn", "2015-01-04", 2),
        ("cali_ckn", "2015-01-04", 3),
        ("mexicana", "2015-01-04", 1)
    ]
    df = spark.createDataFrame(data, ["pizza_type_id", "date", "quantity"])
    # Convert date string to date type
    from pyspark.sql.functions import col, to_date
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    total = get_total_sales_by_date(df, "cali_ckn", "2015-01-04")

    #  Assertions
    assert total == 5