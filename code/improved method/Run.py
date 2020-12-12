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
    dfNames = defaultFiles.dfNames["file_name"].values

    results = {}
    dfNameColumnNamesDict = {}
    limitRow = 10000


    try:
        for i in range(len(dfNames)):
            # try:
            dfName = dfNames[i]
            df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(
                os.path.join(inputPath, dfName)).limit(limitRow)

            print("finished reading the {0} file {1}, rows {2}, columns {3}".format(i, dfName, len(df), len(df.columns)))
            # TODO better to save this and run
            dfNameColumnNamesDict[dfName] = list(df.columns.values)

            spatialColumnDetection = SpatialColumnDetection(df, defaultFiles, i)
            results[dfName] = spatialColumnDetection.detect()
            # TODO: change to logger?
            print("finished the {0} file {1}".format(i, dfName))
            # except:
            #     print("error with the {0} file {1}".format(i, dfName))
    except:
        print("error with the {0} file {1}".format(i, dfName))
        resultsJson = json.dumps(results)
        dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)

        dir_result = os.path.join(outputPath, "result.json")
        dir_dfNameColNamesDict = os.path.join(outputPath, "dfNameColNamesDict.json")
        if not os.path.exists(dir_result):
            open(dir_result, "w").close()
        if not os.path.exists(dir_dfNameColNamesDict):
            open(dir_dfNameColNamesDict, "w").close()
        with open(dir_result, "w") as outfile:
            outfile.write(resultsJson)
        with open(dir_dfNameColNamesDict, "w") as outfile:
            outfile.write(dfNameColNameDictJson)

    dir_result = os.path.join(outputPath, "result.json")
    dir_dfNameColNamesDict = os.path.join(outputPath, "dfNameColNamesDict.json")
    if not os.path.exists(dir_result):
        open(dir_result, "w").close()
    if not os.path.exists(dir_dfNameColNamesDict):
        open(dir_dfNameColNamesDict, "w").close()
    resultsJson = json.dumps(results)
    dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)
    with open(os.path.join(outputPath, "result.json"), "w") as outfile:
        outfile.write(resultsJson)
    with open(os.path.join(outputPath, "dfNameColNamesDict.json"), "w") as outfile:
        outfile.write(dfNameColNameDictJson)
