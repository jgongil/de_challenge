# PART 1: Spark RDD API

## Task 2 - Part a: unique list of products

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise
      
def create_pair(item): 
    return (item, 1) 

# returns list of pairs with unique key (product name), and value set as 1
groceries_summary = groceries_df.rdd\
    .flatMap(list)\
    .map(create_pair)\
    .reduceByKey(lambda a,b: a+b)\
    .filter(lambda x: x[0] != None)\

unique_products = groceries_summary.keys()


# all output files are placed in the same dir
output_path = "dbfs:/tmp/out"
dbutils.fs.mkdirs(output_path)

# list of unique products
filename = os.path.join(output_path,"out_1_2a.txt")
if file_exists is not True:
  unique_products.coalesce(1).saveAsTextFile(filename)

# count of total items
filename = os.path.join(output_path,"out_1_2b.txt")
total_items = sum(groceries_summary.values().collect())

# creates spark dataframe with column name "count" and writes it to file
schema = StructType([StructField("count",IntegerType(),True)])  
data = [(total_items,)]
total_items_df = spark.createDataFrame(data=data, schema=schema)
total_items_df.select("count").coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)