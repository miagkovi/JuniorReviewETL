import kagglehub
import logging
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, to_date, to_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("PizzaSales") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

def run_etl():
    start_time = time.time()
    try:
        path = kagglehub.dataset_download("ylenialongo/pizza-sales")
        logger.info(f"Dataset downloaded to: {path}")

        target_dir = os.path.join(path, "pizza_sales")
        if not os.path.exists(target_dir):
            target_dir = path

        logger.info(f"Using target directory: {target_dir}")
        
        # Download and Read Data
        # Reading CSV files into DataFrames
        opts = {"header": True, "inferSchema": True}
        details = spark.read.options(**opts).csv(os.path.join(target_dir, "order_details.csv"))
        orders = spark.read.options(**opts).csv(os.path.join(target_dir, "orders.csv"))
        pizzas = spark.read.options(**opts).csv(os.path.join(target_dir, "pizzas.csv"))
        types = spark.read.options(**opts).csv(os.path.join(target_dir, "pizza_types.csv"))

        # Joins
        # order_details -> orders (on order_id)
        # order_details -> pizzas (on pizza_id)
        # pizzas -> pizza_types (on pizza_type_id)
        
        df = details.join(orders, "order_id") \
                    .join(pizzas, "pizza_id") \
                    .join(types, "pizza_type_id")

        # Date conversion to proper date format
        # df = df.withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
        # df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        # 1: How many cali_ckn ordered on 2015-01-04
        q1 = df.filter((col("pizza_type_id") == "cali_ckn") & (col("date") == "2015-01-04")) \
               .select(_sum("quantity")).collect()[0][0]
        logger.info("="*40)
        logger.info(f"1) Total cali_ckn ordered on 2015-01-04: {q1 or 0}")

        # 2: Order ingredients 2015-01-02 on 18:27:50
        logger.info("="*40)
        logger.info("2) Ingredients for orders on 2015-01-02 at 18:27:50:")
        df.filter((col("date") == "2015-01-02") & (col("time") == "18:27:50")) \
          .select("name", "ingredients").show(truncate=False)

        # 3: Top selling pizza between (2015-01-01 - 2015-01-08)
        logger.info("="*40)
        logger.info("3) Top selling pizza between (2015-01-01 - 2015-01-08):")
        df.filter(col("date").between("2015-01-01", "2015-01-08")) \
          .groupBy("category") \
          .agg(_sum("quantity").alias("total_qty")) \
          .orderBy(desc("total_qty")).show(1)

        # Save final DataFrame as Parquet
        output_file = "output/pizza_final_view.parquet"
        df.write.mode("overwrite").parquet(output_file)
        logger.info(f"Data saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        logger.info("="*40)
        logger.info(f"Duration: {time.time() - start_time:.2f} seconds")
        spark.stop()

if __name__ == "__main__":
    run_etl()