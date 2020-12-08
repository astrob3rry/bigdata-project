import sys
import json
import pydeequ

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("quality-analysis").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.addPyFile("pydeequ.zip")

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

    fileQueryStr = "`{}`.attributes.*".format(fileName)
    currMetadataDF = metadataDF.select(fileQueryStr)
    # currMetadataDF.show()

    currColumns = fileSparkDF.columns
    for i in range(len(currColumns)):
        print("current column name: {}".format(currColumns[i]))
        columnQueryStr = "`{}`.*".format(currMetadataDF.columns[i])
        colMetadataDF = currMetadataDF.select(columnQueryStr)
        # colMetadataDF.show()
        if colMetadataDF.select("is_spatial").first()[0]:
            currType = colMetadataDF.select("type").first()[0]
            if currType == "borough":
                # get the dataframe, union together with other dfs
                # do a quality analysis after
                # add the data together
                print("borough")
            elif currType == "city":
                print("city")
            elif currType == "state":
                print("state")
            elif currType == "zipcode":
                print("zipcode")

spark.stop()