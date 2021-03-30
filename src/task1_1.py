# PART 1: Spark RDD API

## Task 1: Downloads data file and makes it available to Spark

from pyspark import SparkContext
from pyspark.sql import SparkSession
from os import path
import requests

# Instanciates SparkSession
spark = SparkSession.builder.getOrCreate()

# Parameters
csv_url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
file_name = "groceries.csv"
local_path = "../data/"

def download_file(url, file_name, local_path):
    '''
    Dowloads single file from url.

            Parameters:
                    url (str): url of the site where the file is hosted
                    file_name (srt): Name of the file
                    local_path (str): Target path where the file will be saved

            Returns:
                    local_file (str): Relative path to local file
    '''
    if not path.exists('../data/groceries.csv'):
        res = requests.get(url, allow_redirects=True)
        assert res.status_code == 200, "Failed to download file: {}".format(res.text)
        
        file_content = res.content
        csv_file = open(local_path + file_name, 'wb')
        csv_file.write(file_content)
        csv_file.close()
    return (local_path + file_name)

def csv_to_df(local_file):
    '''
    makes local file available to Spark as pyspark.sql.dataframe.DataFrame.

            Parameters:
                    local_file (str): Relative path to local file

            Returns:
                    DataFrame (str): pyspark.sql.dataframe.DataFrame with csv contents
    '''
    out_df = spark.read.format('csv')\
       .option('header', 'false')\
       .option('inferSchema', 'true')\
       .load(local_file)
    return out_df

local_file = download_file(csv_url, file_name, local_path)
groceries_df = csv_to_df(local_file)