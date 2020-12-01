import pandas as pd
import sys, os
import numpy as np
from snapy import MinHash, LSH
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import random
import re

class SpatialColumnDetection:
    def __init__(self, df, defaultFiles, index):
        self.types = ["longitude", "latitude", "address", "county", "borough", "city", "state",
                      "country",  "zipcode",
                      "other location attribute",
                      "not spatial attribute"]

        self.df = df
        # column names
        self.columnNames = list(df.columns)
        # lower case column names
        self.lcColumnNames = []
        # lower case column name to orginal name map
        self.lowerUpperDict = {}
        self.upperLowerDict = {}
        # each column is what type
        self.colNameType = {}

        self.defaultFiles = defaultFiles
        self.index = index

    def initReturnResult(self):
        self.colNameType["file_index"] = str(self.index)
        self.colNameType["total_spatial_attributes"] = 0
        self.colNameType["attributes"] = {}
        for colName in self.columnNames:
            self.colNameType["attributes"][colName] = {}


    # change all column names to lower cases
    def changeLowerCase(self, ):
        for colName in self.columnNames:
            lwColName = colName.lower()
            self.lcColumnNames.append(lwColName)
            self.lowerUpperDict[lwColName] = colName
            self.upperLowerDict[colName] = lwColName

    def lcs(self, X, Y):
        # find the length of the strings
        m = len(X)
        n = len(Y)

        # declaring the array for storing the dp values
        L = [[None] * (n + 1) for i in range(m + 1)]

        """Following steps build L[m + 1][n + 1] in bottom up fashion 
        Note: L[i][j] contains length of LCS of X[0..i-1] 
        and Y[0..j-1]"""
        for i in range(m + 1):
            for j in range(n + 1):
                if i == 0 or j == 0:
                    L[i][j] = 0
                elif X[i - 1] == Y[j - 1]:
                    L[i][j] = L[i - 1][j - 1] + 1
                else:
                    L[i][j] = max(L[i - 1][j], L[i][j - 1])

                    # L[m][n] contains the length of LCS of X[0..n-1] & Y[0..m-1]
        return L[m][n]

    def editDistance(self, x, y):
        return len(x) + len(y) - 2 * self.lcs(x, y)

    def detect(self, ):
        self.changeLowerCase()
        self.initReturnResult()
        colIndex = 0
        for colName in self.columnNames:
            # change to lower case and trip it
            colNameLwStrip = colName.lower().strip()
            if self.detectLongitude(colNameLwStrip, colName, self.df[colName]):
                type = "longitude"
            elif self.detectLatitude(colNameLwStrip, colName, self.df[colName]):
                type = "latitude"
            elif self.detectAddress(colNameLwStrip, colName):
                type = "address"
            elif self.detectCounty(colNameLwStrip, colName):
                type = "county"
            elif self.detectBorough(colNameLwStrip, colName):
                type = "borough"
            elif self.detectCity(colNameLwStrip, colName):
                type = "city"
            elif self.detectState(colNameLwStrip, colName):
                type = "state"
            elif self.detectCountry(colNameLwStrip, colName):
                type = "country"
            elif self.detectZipcode(colNameLwStrip, colName):
                type = "zipcode"
            elif self.detectOtherLocationAttribute(colNameLwStrip, colName):
                type = "other location attribute"
            else:
                type = "not spatial attribute"

            self.colNameType["attributes"][colName]["type"] = type
            self.colNameType["attributes"][colName]["index"] = colIndex
            colIndex += 1

        count = 0
        for colName in self.columnNames:
            if self.colNameType["attributes"][colName]["type"] != "not spatial attribute":
                count += 1
                self.colNameType["attributes"][colName]["is_spatial"] = True
            else:
                self.colNameType["attributes"][colName]["is_spatial"] = False
        self.colNameType["total_spatial_attributes"] = count

        return self.colNameType

    def commonDetectMethod(self, colNameLw, names, thredshold):
        for name in names:
            if (colNameLw in name) or (name in colNameLw):
                return True
        # use fuzz ratio, which uses the idea of levestain distance, we can specify elsewhere
        for name in names:
            if fuzz.ratio(name, colNameLw) > thredshold:
                return True
        return False

    # TODO: to be finish, colName passed in is lowercase, colNameLw is the lower case and stip()
    def detectLongitude(self, colNameLw, colName, column):
        names = ["longitude", "lon"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # TODO
    def detectLatitude(self, colNameLw, colName, column):
        names = ["latitude", "lat"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # list all the country out, count how many of the sample column values are in country list
    def detectCountry(self, colNameLw,colName):
        # quite similar word
        if "county" in colNameLw:
            return False
        names = ["country"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if self.df[colName].dtype == object:
            dfCountryNames = self.defaultFiles.dfCountryNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            # get the average length of the values
            avgLen = sum(map(len, columnValuesSample)) / len(columnValuesSample)

            # compare with country code, other wise compare with country full name
            if avgLen <= 2.3:
                countries = dfCountryNames["Code"].values
            else:
                countries = dfCountryNames["Name"].values

            # equality count
            count = 0
            for value in columnValuesSample:
                for country in countries:
                    if value.lower() == country.lower():
                        count += 1
                        break
            if count / sampleLength > 0.6:
                return True

        return False

    # list all the state out, fullname and abbreviation, can use sampling
    def detectState(self, colNameLw, colName):
        names = ["state"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if self.df[colName].dtype == object:
            dfStateNames = self.defaultFiles.dfStateNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            # get the average length of the values
            avgLen = sum(map(len, columnValuesSample)) / len(columnValuesSample)

            # compare with state code, other wise compare with state full name
            if avgLen <= 2.3:
                states = dfStateNames["Code"].values
            else:
                states = dfStateNames["Name"].values

            # equality count
            count = 0
            for value in columnValuesSample:
                for state in states:
                    if value.lower() == state.lower():
                        count += 1
                        break
            if count / sampleLength > 0.6:
                return True
        return False

    # can use sampling
    def detectCity(self, colNameLw, colName):
        names = ["city", "town"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if self.df[colName].dtype == object:
            dfCityNames = self.defaultFiles.dfCityNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            cities = dfCityNames["Name"].values
            # equality count
            count = 0
            for value in columnValuesSample:
                for city in cities:
                    if value == city:
                        count += 1
                        break
            if count / sampleLength > 0.6:
                return True
        return False

        # list all the county out, count how many of the sample column values are in county list. Not country!!!

    def detectCounty(self, colNameLw, colName):
        # quite similar word
        if "country" in colNameLw:
            return False
        names = ["county"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if self.df[colName].dtype == object:
            dfCountyNames = self.defaultFiles.dfCountyNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            counties = dfCountyNames["Name"].values
            # equality count
            count = 0
            for value in columnValuesSample:
                for county in counties:
                    if value.lower() == county.lower():
                        count += 1
                        break
            if count / sampleLength > 0.6:
                return True
        return False

    def detectBorough(self, colNameLw, colName):
        names = ["borough"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # need to use sampling
    def detectAddress(self, colNameLw, colName):
        names = ["address", "street", "block"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if self.df[colName].dtype == object:
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            # get the average length of the values
            avgLen = sum(map(len, columnValuesSample)) / len(columnValuesSample)
            # probably not address
            if avgLen < 5:
                return False

            # use regex expression, detect full address
            regexPattern = """
            \b\d{1,6} +.{2,25}\b(avenue|ave|court|ct|street|st|drive|dr|lane|ln|road|rd|blvd|plaza|parkway|pkwy|boulevard|)[.,]?(.{0,25} +\b\d{5}\b)?
            """
            count = 0
            for x in columnValuesSample:
                result = re.match(regexPattern, x)
                if result != None:
                    if result.group() > 10:
                        count += 1
            if count / sampleLength > 0.6:
                return True

            # use regex expression to detect street name like
            regexPattern2 = """
            \d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?
            """
            count = 0
            for x in columnValuesSample:
                result = re.match(regexPattern2, x)
                if result != None:
                    if result.group() > 10:
                        count += 1
            if count / sampleLength > 0.6:
                return True

                # TODO use the addr_detection package
                #             try:
                #                 clf2 = Postal_clf()
                #                 result = clf2.predict(columnValuesSample)
                #             except:
                #                 return False
        return False

    # need to use sampling, and regex
    def detectZipcode(self, colNameLw, colName):
        names = ["zip", "zipcode", "zcode", "postcode"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if (self.df[colName].dtype == object) or (self.df[colName].dtype == np.int64):
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, len(self.df[colName]))
            columnValuesSample = random.sample(list(self.df[colName].values), sampleSize)
            if self.df[colName].dtype == np.int64:
                columnValuesSample = [str(x) for x in columnValuesSample]
            else:
                columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False
            # just us country here, can use other api?
            regexPattern = r"^[0-9]{5}(?:-[0-9]{3})?$"
            matches = []
            for x in columnValuesSample:
                result = re.match(regexPattern, x)
                if result != None:
                    matches.append(bool(x))
            count = sum([bool(x) for x in matches])
            if count / sampleLength > 0.6:
                return True
        return False


    def detectOtherLocationAttribute(self, colNameLw, colName):
        names = ["location", "home", "house", "lot", "bin", "bbl", "nta", "geom", "precinct",
                 "census_tract", "community", "district", "building"]
        thredshold = 70
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False
