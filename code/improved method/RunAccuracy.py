import pandas as pd
import sys, os
import numpy as np
import json

from DefaultFiles import DefaultFiles

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
    with open(os.path.join(outputPath, "result.json")) as f:
        auto = json.loads(f.read())
    with open(os.path.join(outputPath, "dfNameColNamesDict.json")) as f:
        dfNameColumnNamesDict = json.loads(f.read())

    calAccuracy = CalAccuracy(dfNames, manual, auto, dfNameColumnNamesDict)
    accuracy, comparison = calAccuracy.calculate()
    accuracyJson = json.dumps(accuracy)
    comparisonJson = json.dumps(comparison)

    dir_accuracy = os.path.join(outputPath, "accuracy.json")
    if not os.path.exists(dir_accuracy):
        open(dir_accuracy, "w").close()
    with open(dir_accuracy, "w") as outfile:
        outfile.write(accuracyJson)

    dir_comparison = os.path.join(outputPath, "comparison.json")
    if not os.path.exists(dir_comparison):
        open(dir_comparison, "w").close()
    with open(dir_comparison, "w") as outfile:
        outfile.write(comparisonJson)