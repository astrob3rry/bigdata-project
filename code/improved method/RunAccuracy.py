import pandas as pd
import sys, os
import numpy as np
import json
from pyspark.context import SparkContext
import pyspark
from pyspark.sql import SparkSession

from DefaultFiles import DefaultFiles

from CalAccuracy import CalAccuracy
import subprocess

if __name__ == "__main__":
    # input path, for reading the output from Run.py
    inputPath = sys.argv[1]
    # input path for reading from helper
    inputPath2 = sys.argv[2]
    # output path
    outputPath = sys.argv[3]

    defaultFiles = DefaultFiles(inputPath2)
    # TODO can we change values?
    dfNames = defaultFiles.dfNames["file_name"].values

    # calculate accuracy
    spark = SparkSession.builder.appName("RunAccuracy").config("spark.some.config.option", "some-value").getOrCreate()

    dfManual = spark.read.json(os.path.join(inputPath2, "metadata.json"), multiLine = True).toPandas()
    dfAuto = spark.read.json(os.path.join(inputPath, "result.json"), multiLine = True).toPandas()
    dfNames = spark.read.json(os.path.join(inputPath, "dfNameColNamesDict.json"), multiLine = True).toPandas()

    manual = {}
    auto = {}
    dfNameColNamesDict = {}

    for dfName in dfNames:
        colNames = dfNames[dfName][0]
        dfNameColNamesDict[dfName] = colNames
        manualDfName = dfManual[dfName][0]
        manual[dfName] = {}
        manual[dfName]["file_index"] = manualDfName["file_index"]
        manual[dfName]["total_spatial_attributes"] = manualDfName["total_spatial_attributes"]
        manual[dfName]["attributes"] = {}

        autoDfName = dfAuto[dfName][0]
        auto[dfName] = {}
        auto[dfName]["file_index"] = autoDfName["file_index"]
        auto[dfName]["total_spatial_attributes"] = autoDfName["total_spatial_attributes"]
        auto[dfName]["attributes"] = {}

        for colName in colNames:
            try:
                manual[dfName]["attributes"][colName] = {}
                manual[dfName]["attributes"][colName]["index"] = manualDfName["attributes"][colName]["index"]
                manual[dfName]["attributes"][colName]["is_spatial"] = manualDfName["attributes"][colName]["is_spatial"]
                manual[dfName]["attributes"][colName]["type"] = manualDfName["attributes"][colName]["type"]

                auto[dfName]["attributes"][colName] = {}
                auto[dfName]["attributes"][colName]["index"] = autoDfName["attributes"][colName]["index"]
                auto[dfName]["attributes"][colName]["is_spatial"] = autoDfName["attributes"][colName]["is_spatial"]
                auto[dfName]["attributes"][colName]["type"] = autoDfName["attributes"][colName]["type"]
            except:
                print("changing to dictionary error for ", dfName, colName)

    calAccuracy = CalAccuracy(dfNames, manual, auto, dfNameColNamesDict)
    accuracy, comparison = calAccuracy.calculate()
    # accuracyJson = json.dumps(accuracy)
    # comparisonJson = json.dumps(comparison)

    dir_accuracy = os.path.join(outputPath, "accuracy.json")
    dir_comparison = os.path.join(outputPath, "comparison.json")

    df_accuracy = pd.DataFrame([accuracy])
    df_comparison = pd.DataFrame([comparison])

    dfspark_accuracy = spark.createDataFrame(df_accuracy)
    dfspark_comparison = spark.createDataFrame(df_comparison)
    # os.system('echo "{0}" | hadoop fs -put -f - {1}'.format(comparisonJson, dir_comparison))
    # spark.read.json(sc.parallelize(accuracy)).coalesce(1).write.mode('overwrite').json(dir_accuracy)

    dfspark_accuracy.coalesce(1).write.format('json').save(dir_accuracy)
    print("finished saving accuracy.json")
    dfspark_comparison.coalesce(1).write.format('json').save(dir_comparison)
    print("finished saving comparison.json")

