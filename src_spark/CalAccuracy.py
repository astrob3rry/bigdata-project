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
        self.types = Settings.types


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

        # count of each type
        manualType = {}
        autoType = {}
        typeEqualCount = {}
        # init these two:
        for type in self.types:
            manualType[type] = 0
            autoType[type] = 0
            typeEqualCount[type] = 0

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
                    elif not (infoManual["is_spatial"]) and (infoAuto["is_spatial"]):
                        falsePositive += 1
                        totalNegative += 1
                    elif (infoManual["is_spatial"]) and not (infoAuto["is_spatial"]):
                        falseNegative += 1
                        totalPositive += 1
                    elif not (infoManual["is_spatial"]) and not (infoAuto["is_spatial"]):
                        trueNegative += 1
                        totalNegative += 1
                        ["longitude", "latitude", "address", "county", "borough", "city", "state",
                         "country", "zipcode",
                         "other location attribute",
                         "not spatial attribute"]

                except:
                    print("is_spatial error in file: ", dfName, "in column: ", colName)
                    continue

                # count each type
                try:
                    type = self.nameNormalize[infoManual["type"]]
                    if (type == infoAuto["type"]):
                        typeEqualCount[type] += 1
                    manualType[type] += 1
                    autoType[infoAuto["type"]] += 1
                except:
                    print("type error in metadata file: ", dfName, "in column: ", colName)
                    manualType[self.nameNormalize[infoManual["type"]]] += 1
                    autoType[infoAuto["type"]] += 1
                    pass

                totalColumn += 1
        result = {}
        result["totalColumn"] = totalColumn
        result["truePositive"] = truePositive
        result["trueNegative"] = trueNegative
        result["falsePositive"] = falsePositive
        result["falseNegative"] = falseNegative
        result["typeEqualCount"] = typeEqualCount
        result["totalPositive"] = totalPositive
        result["totalNegative"] = totalNegative
        result["manualType"] = manualType
        result["autoType"] = autoType
        result["typeEqualCount"] = typeEqualCount

        return result


