# PART 2: Spark Dataframe API

## Task 1: Downloads parquet file and make it available to Spark

from pyspark import SparkContext,SparkFiles
from pyspark.sql import SparkSession
import csv

# instanciates spark session object
spark = SparkSession.builder.getOrCreate()

url = "https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet/"
file_name = "sf-airbnb-clean.parquet"
file_path = "../data/"

airbnb_df = spark.read.parquet(file_path + file_name)