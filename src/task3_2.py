## Task 2: MLlib

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, OneHotEncoder, IndexToString
from pyspark import SparkContext
from pyspark.sql import SparkSession

#! curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o /tmp/iris.csv

# Instanciates SparkSession
spark = SparkSession.builder.getOrCreate()
local_file = "../data/iris.csv"
col_names = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]

schema = """`sepal_length` DOUBLE,
            `sepal_width` DOUBLE,
            `petal_length` DOUBLE,
            `petal_width` DOUBLE,
            `class` STRING
        """

df = spark.read.csv(local_file,schema=schema)
    
categoricalCols = ["class"]

# The following two lines are estimators. They return functions that we will later apply to transform the dataset.

# Convert it to a numeric value using StringIndexer.
labelToIndex = StringIndexer(inputCol="class", outputCol="indexed_class")
labelIndexer = labelToIndex.fit(df)
labelReverser = IndexToString(inputCol="prediction", outputCol="class", labels=labelIndexer.labels)

# This includes both the numeric columns and the one-hot encoded binary vector columns in our dataset.
numericCols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

vecAssembler = VectorAssembler(inputCols=numericCols, outputCol="features")

lr = LogisticRegression(featuresCol="features", labelCol="indexed_class", regParam=1e5)

# Define the pipeline based on the stages created in previous steps.
pipeline = Pipeline(stages=[labelToIndex, vecAssembler, lr, labelReverser])

# Define the pipeline model.
pipelineModel = pipeline.fit(df)

test_df = spark.createDataFrame([
    (5.1, 3.5, 1.4, 0.2),
    (6.2, 3.4, 5.4, 2.3)
], ["sepal_length", "sepal_width", "petal_length", "petal_width"])

# Apply the pipeline model to the test dataset.
pred_df = pipelineModel.transform(test_df)

filename = '../out/out_3_2.txt'
pred_df.select("class").coalesce(1)\
    .write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save(filename)
        
pred_df.select("class").show()