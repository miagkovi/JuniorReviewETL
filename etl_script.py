import kagglehub
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, to_date

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
        
        # 1. Download and Read Data
        # Reading CSV files into DataFrames
        opts = {"header": True, "inferSchema": True}
        details = spark.read.options(**opts).csv(f"{path}/order_details.csv")
        orders = spark.read.options(**opts).csv(f"{path}/orders.csv")
        pizzas = spark.read.options(**opts).csv(f"{path}/pizzas.csv")
        types = spark.read.options(**opts).csv(f"{path}/pizza_types.csv")

        # 2. Joins
        # order_details -> orders (on order_id)
        # order_details -> pizzas (on pizza_id)
        # pizzas -> pizza_types (on pizza_type_id)
        
        df = details.join(orders, "order_id") \
                    .join(pizzas, "pizza_id") \
                    .join(types, "pizza_type_id")

        # Date conversion
        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        print("\n" + "="*30 + "\nReport\n" + "="*30)

        # 1: How many cali_ckn ordered at 2015-01-04
        q1 = df.filter((col("pizza_type_id") == "cali_ckn") & (col("date") == "2015-01-04")) \
               .select(_sum("quantity")).collect()[0][0]
        print(f"1. cali_ckn pizza ordered at 2015-01-04: {q1 or 0}")

        # 2: Order ingredients 2015-01-02 at 18:27:50
        q2 = df.filter((col("date") == "2015-01-02") & (col("time") == "18:27:50")) \
          .select("name", "ingredients").show(truncate=False)
        print(f"2. Order ingredients at 18:27:50: {q2}")

        # 3: Top selling pizza between (2015-01-01 - 2015-01-08)
        q3 = df.filter(col("date").between("2015-01-01", "2015-01-08")) \
          .groupBy("category") \
          .agg(_sum("quantity").alias("total_qty")) \
          .orderBy(desc("total_qty")).show(1)
        print(f"3. Top selling pizza between (2015-01-01 - 2015-01-08): {q3}")

        # Save final DataFrame as Parquet
        df.write.mode("overwrite").parquet("output/pizza_final_view.parquet")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        logger.info(f"Duration: {time.time() - start_time:.2f} seconds")
        spark.stop()

if __name__ == "__main__":
    run_etl()