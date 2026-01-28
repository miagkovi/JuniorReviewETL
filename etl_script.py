import kagglehub
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Logging configuration
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

spark = SparkSession.builder \
    .appName("JuniorReviewETL") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

def run_etl():
    start_time = time.time()
    try:
        # 1. Loading data from Kaggle
        path = kagglehub.dataset_download("iamsouravbanerjee/customer-shopping-trends-dataset")
        
        # Read CSV files into DataFrame
        df = spark.read.csv(f"{path}/*.csv", header=True, inferSchema=True)
        logging.info(f"Data loaded successfully. Rows: {df.count()}")

        # 2. Transformation
        # Filtering and adding a timestamp
        processed_df = df.filter(col("Age") > 18) \
                         .withColumn("processed_at", current_timestamp())

        # 3. Simple report (Aggregation)
        report_df = processed_df.groupby("Category") \
                                .agg({"Purchase Amount (USD)": "sum"}) \
                                .withColumnRenamed("sum(Purchase Amount (USD))", "total_sales")

        # 4. Saving the report as Parquet
        report_df.write.mode("overwrite").parquet("output/sales_report.parquet")
        
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"Error in ETL: {str(e)}")
    finally:
        # Statistics on resources and time
        duration = time.time() - start_time
        logging.info(f"Execution time: {duration:.2f} sec")
        # Display resource configuration
        executor_memory = spark.conf.get("spark.executor.memory")
        logging.info(f"Memory used: {executor_memory}")

if __name__ == "__main__":
    run_etl()
    spark.stop()