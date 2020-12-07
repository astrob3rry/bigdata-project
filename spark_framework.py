import sys
import json
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()

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
    hdfsPath = "data/" + fileName
    fileSparkDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(hdfsPath)
    # print("current schema: {}".format(currDF.printSchema()))
    ### turn spark DF to pandas DF
    filePandasDF = fileSparkDF.toPandas()
    ### output the columns
    colNames = filePandasDF.columns.values
    for col in colNames:
        print("curr columns: {}".format(col))

spark.stop()