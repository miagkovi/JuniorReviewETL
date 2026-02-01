from pyspark.sql import SparkSession
from src.downloader import DatasetDownloader
from src.transformations import (
    build_pizza_sales_flat_table, 
    get_total_sales_by_date, 
    get_ingredients_by_time,
    get_top_category_in_range
)
from src.utils import get_logger, log_resource_usage
import time

logger = get_logger("PizzaETL")

def main():
    start_time = time.time()
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PizzaSalesAnalysis") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # 1. Download dataset
        downloader = DatasetDownloader()
        path = downloader.fetch_data_path()
        
        # 2. Read CSV files
        opts = {"header": True, "inferSchema": True}
        details = spark.read.options(**opts).csv(f"{path}/order_details.csv")
        orders  = spark.read.options(**opts).csv(f"{path}/orders.csv")
        pizzas  = spark.read.options(**opts).csv(f"{path}/pizzas.csv")
        types   = spark.read.options(**opts).csv(f"{path}/pizza_types.csv")
        
        # 3. Transform data
        flat_df = build_pizza_sales_flat_table(details, orders, pizzas, types)

        # 1
        q1 = get_total_sales_by_date(flat_df, "cali_ckn", "2015-01-04")
        logger.info(f"1. Count 'cali_ckn' on 2015-01-04: {q1}")

        # 2
        logger.info("2. Ingredients 2015-01-02 18:27:50:")
        ingredients_df = get_ingredients_by_time(flat_df, "2015-01-02", "18:27:50")
        ingredients_df.show(truncate=False)

        # 3
        logger.info("3. Top category (01.01 - 08.01):")
        top_cat = get_top_category_in_range(flat_df, "2015-01-01", "2015-01-08")
        top_cat.show(1)

        # Save final table
        flat_df.write.mode("overwrite").parquet("output/pizza_final.parquet")
        logger.info("Final table saved to Parquet.")
        
    except Exception as e:
        logger.error(f"Error in ETL: {e}")
    finally:
        # Resource usage logging and cleanup
        log_resource_usage(logger)
        logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
        spark.stop()

if __name__ == "__main__":
    main()