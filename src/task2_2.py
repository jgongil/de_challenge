# PART 2: Spark Dataframe API

## Task 2: Creates CSV that lists the minimum price, maximum price and total row count

summary = airbnb_df\
    .groupBy()\
    .min('price')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df\
    .groupBy()\
    .max('price')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df.count()

filename = os.path.join(output_path,"out_2_2.txt")
schema = StructType([StructField("min_price",FloatType(),True),\
                     StructField("max_price",FloatType(),True),\
                     StructField("total",IntegerType(),True)])  

summary_df = spark.createDataFrame(data=[summary], schema=schema)
summary_df.coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)