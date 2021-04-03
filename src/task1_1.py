# PART 1: Spark RDD API

## Task 1: Downloads data file and makes it available to Spark

from pyspark.sql import SparkSession
import os
import requests

# Instanciates SparkSession
spark = SparkSession.builder.getOrCreate()

# Parameters
csv_url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
file_name = "groceries.csv"
dbfs_path = "dbfs:/tmp/input_data/"


def download_file(url, file_name, dbfs_path):
    '''
    Dowloads single file from url and push it to dbfs.

            Parameters:
                    url (str): url of the site where the file is hosted
                    file_name (srt): Name of the file
                    dbfs_path (str): Target path where the file will be saved

            Returns:
                    target_path (str): path to the saved file
    '''
    # gets file from url
    res = requests.get(url, allow_redirects=True)
    assert res.status_code == 200, "Failed to download file: {}".format(res.text)
    file_content = res.text
    # puts file into dbfs
    target_path = os.path.join(dbfs_path,file_name)
    dbutils.fs.put(target_path,file_content, overwrite=True)
    return (target_path)

def csv_to_df(dbfs_file):
    '''
    makes local file available to Spark as pyspark.sql.dataframe.DataFrame.

            Parameters:
                    dbfs_file (str): Path to source file

            Returns:
                    DataFrame (str): pyspark.sql.dataframe.DataFrame with csv contents
    '''
    out_df = spark.read.format('csv')\
       .option('header', 'false')\
       .option('inferSchema', 'true')\
       .load(dbfs_file)
    return out_df

# downloads csv file into dbfs
dbfs_file = download_file(csv_url, file_name, dbfs_path)

# loads file into dataframe
groceries_df = csv_to_df(dbfs_file)