import json
import Settings as Settings
class CalAccuracy:
    def __init__(self, dfNames, manual, auto, dfNameColumnNamesDict):
        self.dfNames = dfNames
        self.manual = manual
        self.auto = auto
        # every dataframe's column names
        self.dfNameColumnNamesDict = dfNameColumnNamesDict
        # type in manual change to standard type description first
        self.nameNormalize = Settings.nameNormalize

    def calculate(self):
        totalColumn = 0
        truePositive = 0
        trueNegative = 0
        falsePositive = 0
        falseNegative = 0
        # whether the type is the same
        typeEqual = 0
        totalPositive = 0
        totalNegative = 0
        # total valid column number

        for dfName in self.dfNames:
            for colName in self.dfNameColumnNamesDict[dfName]:
                try:
                    infoManual = self.manual[dfName]["attributes"][colName]
                    infoAuto = self.auto[dfName]["attributes"][colName]
                except:
                    print("error in file: ", dfName, "in column: ", colName)
                    continue

                try:
                    if (infoManual["is_spatial"]) and (infoAuto["is_spatial"]):
                        truePositive += 1
                        totalPositive += 1
                        if (self.nameNormalize[infoManual["type"]] == infoAuto["type"]):
                            typeEqual += 1
                    elif not (infoManual["is_spatial"]) and (infoAuto["is_spatial"]):
                        falsePositive += 1
                        totalNegative += 1
                    elif (infoManual["is_spatial"]) and not (infoAuto["is_spatial"]):
                        falseNegative += 1
                        totalPositive += 1
                    elif not (infoManual["is_spatial"]) and not (infoAuto["is_spatial"]):
                        trueNegative += 1
                        totalNegative += 1
                except:
                    print("is_spatial error in file: ", dfName, "in column: ", colName)
                    continue
                totalColumn += 1
        result = {}
        result["totalColumn"] = totalColumn
        result["truePositive"] = truePositive
        result["trueNegative"] = trueNegative
        result["falsePositive"] = falsePositive
        result["falseNegative"] = falseNegative
        result["typeEqual"] = typeEqual
        result["totalPositive"] = totalPositive
        result["totalNegative"] = totalNegative

        return result


