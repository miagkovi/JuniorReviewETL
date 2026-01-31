from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, desc, to_date

def build_pizza_sales_flat_table(details: DataFrame, orders: DataFrame, pizzas: DataFrame, types: DataFrame) -> DataFrame:
    """Join all tables into a single denormalized view."""
    # Convert date string to date type
    orders_processed = orders.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    
    return details.join(orders_processed, "order_id") \
                  .join(pizzas, "pizza_id") \
                  .join(types, "pizza_type_id")

def get_total_sales_by_date(df: DataFrame, pizza_type: str, target_date: str) -> float:
    """Get total sales of a specific pizza type on a given date."""
    result = df.filter((col("pizza_type_id") == pizza_type) & (col("date") == target_date)) \
               .agg(_sum("quantity")).collect()
    return result[0][0] if result and result[0][0] else 0

def get_ingredients_by_time(df: DataFrame, target_date: str, target_time: str) -> DataFrame:
    """Get ingredients for a specific order time."""
    return df.filter((col("date") == target_date) & (col("time") == target_time)) \
             .select("name", "ingredients").distinct()

def get_top_category_in_range(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Get top category for a date range."""
    return df.filter(col("date").between(start_date, end_date)) \
             .groupBy("category") \
             .agg(_sum("quantity").alias("total")) \
             .orderBy(desc("total"))