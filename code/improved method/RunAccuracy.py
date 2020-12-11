import pandas as pd
import sys, os
import numpy as np
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()

from DefaultFiles import DefaultFiles
from SpatialColumnDetection import SpatialColumnDetection
from CalAccuracy import CalAccuracy

if __name__ == "__main__":
    # input path, the helper path, TODO change to hdfs path
    inputPath = sys.argv[1]
    # output path
    outputPath = sys.argv[2]

    defaultFiles = DefaultFiles(inputPath)
    # TODO can we change values?
    dfNames = defaultFiles.dfNames["file_name"].values

    # calculate accuracy
    with open(os.path.join(inputPath, "metadata.json")) as f:
        manual = json.loads(f.read())

    if not os.path.exists(os.path.join(outputPath, "result.json")):
        open(os.path.join(outputPath, "result.json"), "w").close()
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