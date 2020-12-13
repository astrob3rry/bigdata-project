import pandas as pd
import sys, os
import numpy as np
import json

from DefaultFiles import DefaultFiles
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()

from CalAccuracy import CalAccuracy
from SpatialColumnDetection import SpatialColumnDetection
if __name__ == "__main__":
    dt_fmt = "%m/%d/%Y %H:%M:%S"
    # input path, TODO change to hdfs path
    inputPath = sys.argv[1]
    # input path 2 for default files
    inputPath2 = sys.argv[2]
    # output path
    outputPath = sys.argv[3]

    defaultFiles = DefaultFiles(inputPath2)
    dfNames = defaultFiles.dfNames.select("file_name").rdd.flatMap(lambda x: x).collect()

    results = {}
    dfNameColumnNamesDict = {}
    limitRow = 10000


    try:
        for i in range(len(dfNames)):
            try:
                dfName = dfNames[i]
                df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(
                    os.path.join(inputPath, dfName)).limit(limitRow)

                print("finished reading the {0} file {1}, rows {2}, columns".format(i, dfName,
                                                                                    df.count(), len(df.columns)))
                # TODO better to save this and run
                dfNameColumnNamesDict[dfName] = [colName for colName in df.columns]

                spatialColumnDetection = SpatialColumnDetection(df, defaultFiles, i)
                results[dfName] = spatialColumnDetection.detect()
                # TODO: change to logger?
                print("finished the {0} file {1}".format(i, dfName))
            except:
                print("error with the {0} file {1}".format(i, dfName))
    except:
        print("error with the {0} file {1}".format(i, dfName))
        # resultsJson = json.dumps(results)
        # dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)

        dir_result = os.path.join(outputPath, "result.json")
        dir_dfNameColNamesDict = os.path.join(outputPath, "dfNameColNamesDict.json")

        df_result = pd.DataFrame([results])
        df_dfNameColNamesDict = pd.DataFrame([dfNameColumnNamesDict])

        dfspark_result = spark.createDataFrame(df_result)
        dfspark_dfNameColNamesDict = spark.createDataFrame(df_dfNameColNamesDict)

        dfspark_result.coalesce(1).write.format('json').save(dir_result)
        print("finished saving result.json")
        dfspark_dfNameColNamesDict.coalesce(1).write.format('json').save(dir_dfNameColNamesDict)
        print("finished saving dfNameColNamesDict.json")

    # save file after finished
    dir_result = os.path.join(outputPath, "result.json")
    dir_dfNameColNamesDict = os.path.join(outputPath, "dfNameColNamesDict.json")

    df_result = pd.DataFrame([results])
    df_dfNameColNamesDict = pd.DataFrame([dfNameColumnNamesDict])

    dfspark_result = spark.createDataFrame(df_result)
    dfspark_dfNameColNamesDict = spark.createDataFrame(df_dfNameColNamesDict)

    dfspark_result.coalesce(1).write.format('json').save(dir_result)
    print("finished saving result.json")
    dfspark_dfNameColNamesDict.coalesce(1).write.format('json').save(dir_dfNameColNamesDict)
    print("finished saving dfNameColNamesDict.json")


