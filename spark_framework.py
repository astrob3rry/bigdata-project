'''
module load python/gnu/3.4.4
module load spark/2.4.0
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python spark_framework.py metadata.json
'''

import sys
import json
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()
# org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 18 tasks (1052.8 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
# spark.conf.set("spark.driver.maxResultSize", "10g")
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")

### put "metadata.json" in the spark-submit argument
path = sys.argv[1]
# print("path is: {}".format(path))

### read "metadata.json" into a spark DF
metadataDF = spark.read.json(path, multiLine = True)
# metadataDF.printSchema()

### get a list of file names from the spark DF
fileNames = metadataDF.columns
# print ("file names type: {}".format(type(fileNames)))
# print("fileNames: {}".format(fileNames))

### read each file into a spark DF, and process them one by one accordingly
for fileName in fileNames:
    print("***********current file name: {}***********".format(fileName))
    hdfsPath = "data/" + fileName
    fileSparkDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(hdfsPath)
    # print("current schema: {}".format(fileSparkDF.printSchema()))

    columns = fileSparkDF.columns
    for col in columns:
        print("current column name: {}".format(col))

    ### turn spark DF to pandas DF(does not work if the file is too large)
    # filePandasDF = fileSparkDF.toPandas()
    ### output the columns
    # colNames = filePandasDF.columns.values
    # for col in colNames:
    #     print("current column name: {}".format(col))
    ### create a spark DF from a pandas DF
    # ResultSparkDF = spark.createDataFrame(filePandasDF)

spark.stop()
