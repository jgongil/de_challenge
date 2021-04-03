# PART 2: Spark Dataframe API

## Task 1: Downloads parquet file and make it available to Spark

from pyspark import SparkContext,SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
import os

# instanciates spark session object
spark = SparkSession.builder.getOrCreate()

url = "https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet/"
file_path = "dbfs:/FileStore/tables/sf-airbnb-clean/"
output_path = "dbfs:/tmp/out"
dbutils.fs.mkdirs(output_path)

airbnb_df = spark.read.parquet(file_path)