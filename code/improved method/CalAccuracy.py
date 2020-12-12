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
        comparison = {}
        autoDetails = {}
        typePrecision = {}
        typeRecall = {}
        # init these two:
        for type in self.types:
            manualType[type] = 0
            autoType[type] = 0
            typeEqualCount[type] = 0
            autoDetails[type] = {}
            autoDetails[type]["TP"] = 0
            autoDetails[type]["FP"] = 0
            autoDetails[type]["FN"] = 0

            typePrecision[type] = 0
            typeRecall[type] = 0

        for dfName in self.dfNames:
            comparison[dfName] = {}
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


                    typeM = self.nameNormalize[infoManual["type"]]
                    typeA = infoAuto["type"]
                    flag = ""
                    for type in self.types:
                        if typeM == type:
                            if typeM == typeA:
                                flag = "TP"
                                autoDetails[infoAuto["type"]][flag] += 1
                            else:
                                flag = "FN"
                                autoDetails[infoAuto["type"]][flag] += 1
                            break
                        if (typeA == type) and (typeM != type):
                            flag = "FP"
                            autoDetails[infoAuto["type"]][flag] += 1
                            break
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
                    comparison[dfName][colName] = (type, infoAuto["type"], flag)
                except:
                    print("type error in metadata file: ", dfName, "in column: ", colName)
                    manualType[infoManual["type"]] += 1
                    autoType[infoAuto["type"]] += 1
                    comparison[dfName][colName] = (infoManual["type"], infoAuto["type"], flag)
                    pass

                totalColumn += 1

        for type in self.types:
            typePrecision[type] = autoDetails[type]["TP"] / (autoDetails[type]["TP"] + autoDetails[type]["FP"])
            typeRecall[type] = autoDetails[type]["TP"] / (autoDetails[type]["TP"] + autoDetails[type]["FN"])

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
        result["autoDetails"] = autoDetails
        result["typePrecision"] = typePrecision
        result["typeRecall"] = typeRecall

        return result, comparison


