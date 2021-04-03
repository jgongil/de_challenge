# PART 2: Spark Dataframe API

## Task 4: How many people can be accomodated by the property with the lowest price and highest rating?

naccomodates_bestdeal = airbnb_df\
    .orderBy(airbnb_df.price.asc(),airbnb_df.review_scores_rating.desc())\
    .select('accommodates')\
    .take(1)[0]\
    .__getitem__(0)

filename = os.path.join(output_path,"out_2_4.txt")
schema = StructType([StructField("n_people",FloatType(),True)])
data = [(naccomodates_bestdeal,)]
naccomodates_bestdeal_df = spark.createDataFrame(data=data, schema=schema)
naccomodates_bestdeal_df.coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)