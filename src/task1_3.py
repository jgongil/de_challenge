# PART 1: Spark RDD API

## Task 3: Top 5 products

filename = os.path.join(output_path,"out_1_3.txt")
top5groceries = groceries_summary.takeOrdered(5,lambda x: -x[1])

# creates spark dataframe with column name "count" and writes it to file
schema = StructType([StructField("product",StringType(),True),\
                     StructField("count",IntegerType(),True)])  
top5groceries_df = spark.createDataFrame(data=top5groceries, schema=schema)
top5groceries_df.coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)