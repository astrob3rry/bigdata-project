import pandas as pd
import sys, os
import numpy as np
import json
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()

from DefaultFiles import DefaultFiles
from SpatialColumnDetection import SpatialColumnDetection
from CalAccuracy import CalAccuracy

if __name__ == "__main__":
    dt_fmt = "%m/%d/%Y %H:%M:%S"
    # input path, TODO change to hdfs path
    inputPath = sys.argv[1]
    # input path 2 for default files
    inputPath2 = sys.argv[2]
    # output path
    outputPath = sys.argv[3]
    # only read limited rows of data
    limitRow = 10000

    defaultFiles = DefaultFiles(inputPath2)
    # TODO can we change values?
    dfNames = defaultFiles.dfNames.select("file_name").rdd.flatMap(lambda x: x).collect()

    results = {}
    dfNameColumnNamesDict = {}
    try:
        for i in range(len(dfNames)):
            # try:
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
            print("finished processing the {0} file {1}".format(i, dfName))
    except:
        # save the result if there is error

        print("error with the {0} file {1}".format(i, dfName))
        resultsJson = json.dumps(results)
        dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)

        now = datetime.datetime.now().strftime(dt_fmt)
        
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



    resultsJson = json.dumps(results)
    dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)
    now = datetime.datetime.now().strftime(dt_fmt)
        
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


    # calculate accuracy
    with open(os.path.join(inputPath2, "metadata.json")) as f:
        manual = json.loads(f.read())
    with open(os.path.join(outputPath, "result.json")) as f:
        auto = json.loads(f.read())
    with open(os.path.join(outputPath, "dfNameColNamesDict.json")) as f:
        dfNameColumnNamesDict = json.loads(f.read())

    calAccuracy = CalAccuracy(dfNames, manual, auto, dfNameColumnNamesDict)
    accuracy = calAccuracy.calculate()
    accuracyJson = json.dumps(accuracy)

    dir_accuracy = os.path.join(outputPath, "accuracy.json")
    if not os.path.exists(dir_accuracy):
        open(dir_accuracy, "w").close()
    with open(dir_accuracy, "w") as outfile:
        outfile.write(accuracyJson)