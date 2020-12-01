import pandas as pd
import sys, os
import numpy as np
import json

from DefaultFiles import DefaultFiles
from SpatialColumnDetection import SpatialColumnDetection
from CalAccuracy import CalAccuracy

if __name__ == "__main__":
    # input path
    inputPath = os.path.join(os.getcwd(), "data")
    # output path
    outputPath = os.path.join(os.getcwd(), "out")

    defaultFiles = DefaultFiles(inputPath)
    dfNames = defaultFiles.dfNames["fileName"].values
    results = {}
    dfNameColumnNamesDict = {}
    for i in range(len(dfNames)):
        # try:
        dfName = dfNames[i]
        df = pd.read_csv(os.path.join(inputPath, dfName))
        # TODO better to save this and run
        dfNameColumnNamesDict[dfName] = list(df.columns.values)

        spatialColumnDetection = SpatialColumnDetection(df, defaultFiles, i)
        results[dfName] = spatialColumnDetection.detect()
        # TODO: change to logger?
        print("finished the {0} file {1}".format(i, dfName))
        # except:
        #     print("error with the {0} file {1}".format(i, dfName))

    resultsJson = json.dumps(results)
    dfNameColNameDictJson = json.dumps(dfNameColumnNamesDict)
    with open(os.path.join(outputPath, "result.json"), "w") as outfile:
        outfile.write(resultsJson)
    with open(os.path.join(outputPath, "dfNameColNamesDict.json"), "w") as outfile:
        outfile.write(dfNameColNameDictJson)


    # calculate accuracy
    with open(os.path.join(inputPath, "metadata.json")) as f:
        manual = json.loads(f.read())
    with open(os.path.join(outputPath, "result.json")) as f:
        auto = json.loads(f.read())
    with open(os.path.join(outputPath, "dfNameColNamesDict.json")) as f:
        dfNameColumnNamesDict = json.loads(f.read())

    calAccuracy = CalAccuracy(dfNames, manual, auto, dfNameColumnNamesDict)
    accuracy = calAccuracy.calculate()
    accuracyJson = json.dumps(accuracy)
    with open(os.path.join(outputPath, "accuracy.json"), "w") as outfile:
        outfile.write(accuracyJson)