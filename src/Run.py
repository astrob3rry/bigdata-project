import pandas as pd
import sys, os
import numpy as np
import json

from DefaultFiles import DefaultFiles

from CalAccuracy import CalAccuracy
from SpatialColumnDetection import SpatialColumnDetection
if __name__ == "__main__":
    # input path
    inputPath = os.path.join(os.getcwd(), "data")
    # output path
    outputPath = os.path.join(os.getcwd(), "out")

    defaultFiles = DefaultFiles(inputPath)
    dfNames = defaultFiles.dfNames["file_name"].values
    results = {}
    dfNameColumnNamesDict = {}
    limitRow = 10000


    try:
        for i in range(len(dfNames)):
            # try:
            dfName = dfNames[i]
            df = pd.read_csv(os.path.join(inputPath, dfName), nrows = limitRow)
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
