from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging

logging.basicConfig(level=logging.INFO)

builder = SparkSession.builder \
    .appName("DeltaLakePipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

logging.info(f"Spark Session created with app name: {spark.sparkContext.appName}")