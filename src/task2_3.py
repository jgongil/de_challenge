# PART 2: Spark Dataframe API

## Task 3: Calculate the average number of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000 and a review score being exactly equalt to 10.

selected_df = airbnb_df\
    .filter((airbnb_df.price>5000 ) & (airbnb_df.review_scores_value==10))

avg_summary = selected_df\
    .groupBy()\
    .avg('bathrooms')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df\
    .groupBy()\
    .avg('bedrooms')\
    .collect()[0]\
    .__getitem__(0)


filename = os.path.join(output_path,"out_2_3.txt")
schema = StructType([StructField("bathrooms",FloatType(),True),\
                     StructField("bedrooms",FloatType(),True)])  
avg_summary_df = spark.createDataFrame(data=[avg_summary], schema=schema)
avg_summary_df.coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)