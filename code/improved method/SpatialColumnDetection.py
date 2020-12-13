import pandas as pd
import sys, os
import numpy as np
from snapy import MinHash, LSH
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import random
import re
import Settings as Settings

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.addPyFile("/home/wh2099/project/mmh3.py")
spark.sparkContext.addPyFile("/home/wh2099/project/snapy.zip")
import mmh3
from snapy import MinHash, LSH

class SpatialColumnDetection:
    def __init__(self, df, defaultFiles, index):
        self.types = Settings.types

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
        # get the dtype dictionary first
        self.dtypes = {}
        for keyVal in df.dtypes:
            self.dtypes[keyVal[0]] = keyVal[1]

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
            if self.detectLongitude(colNameLwStrip, colName):
                type = "longitude"
            elif self.detectLatitude(colNameLwStrip, colName):
                type = "latitude"
            elif self.detectAddress(colNameLwStrip, colName):
                type = "address"
            # elif self.detectCounty(colNameLwStrip, colName):
            #     type = "county"
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
    def detectLongitude(self, colNameLw, colName):
        names = ["longitude", "lon"]
        thredshold = 75
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # TODO: use datamart
    def detectLatitude(self, colNameLw, colName):
        names = ["latitude"]
        thredshold = 75
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # list all the country out, count how many of the sample column values are in country list
    def detectCountry(self, colNameLw,colName):
        # quite similar word
        if "county" in colNameLw:
            return False
        names = ["country"]
        thredshold = 100
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True

        # this not work for "yes" or "no" column because "no" stands for norway
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
            dfCountryNames = self.defaultFiles.dfCountryNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            # get the average length of the values
            avgLen = sum(map(len, columnValuesSample)) / len(columnValuesSample)

            # compare with country code, other wise compare with country full name
            if avgLen <= 2.3:
                countries = dfCountryNames.select("Code").rdd.flatMap(lambda x: x).collect()
                countries = [code for code in countries if code != "NO"]
            else:
                countries = dfCountryNames.select("Name").rdd.flatMap(lambda x: x).collect()
                countries = [code for code in countries if code != "Norway"]

            # equality count
            count = 0
            for value in columnValuesSample:
                for country in countries:
                    if value.lower() == country.lower():
                        count += 1
                        break
            if count / sampleLength > 0.7:
                return True

        return False

    # list all the state out, fullname and abbreviation, can use sampling
    def detectState(self, colNameLw, colName):
        names = ["state"]
        thredshold = 90
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
            dfStateNames = self.defaultFiles.dfStateNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            # get the average length of the values
            avgLen = sum(map(len, columnValuesSample)) / len(columnValuesSample)

            # compare with state code, other wise compare with state full name
            if avgLen <= 2.3:
                states = dfStateNames.select("Code").rdd.flatMap(lambda x: x).collect()
            else:
                states = dfStateNames.select("Name").rdd.flatMap(lambda x: x).collect()

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
        thredshold = 90
        # if self.commonDetectMethod(colNameLw, names, thredshold):
        #     return True
        # avoid capcity, ethnicity, electricity words
        for name in names:
            if colNameLw == name:
                return True
        # some column name states, county but have New York inside
        if self.commonDetectMethod(colNameLw, ["state", "county"], thredshold):
            return False
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
            dfCityNames = self.defaultFiles.dfCityNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meanning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            cities = dfCityNames.select("Name").rdd.flatMap(lambda x: x).collect()
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
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
            dfCountyNames = self.defaultFiles.dfCountyNames
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
            columnValuesSample = [x for x in columnValuesSample if type(x) == str]
            sampleLength = len(columnValuesSample)
            # meaning many are nan
            if sampleLength / sampleSize < 0.1:
                return False

            counties = dfCountyNames.select("Name").rdd.flatMap(lambda x: x).collect()
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
        names = ["borough", "boro", "borocode"]
        thredshold = 80
        if self.commonDetectMethod(colNameLw, names, thredshold):
            return True
        return False

    # need to use sampling
    def detectAddress(self, colNameLw, colName):
        names = ["address", "street", "block"]
        thredshold = 80
        if self.commonDetectMethod(colNameLw, names, thredshold):
            # add one more condition
            if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
                return True
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string"):
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
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
            regexPattern ="""
            \b\d{1,6} +.{2,25}\b(avenue|ave|court|ct|street|st|
            drive|dr|lane|ln|road|rd|blvd|plaza|parkway|pkwy|
            boulevard|)[.,]?(.{0,25} +\b\d{5}\b)?
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
            regexPattern2 ="""
            \d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|
            Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?
            """
            count = 0
            for x in columnValuesSample:
                result = re.match(regexPattern2, x)
                if result != None:
                    if result.group() > 10:
                        count += 1
            if count / sampleLength > 0.7:
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
        if (self.dtypes[colName] == object) or (self.dtypes[colName] == "string") or \
            (self.dtypes[colName] == "int") or (self.dtypes[colName] == "bigint"):
            # sampling and pair wise comparison
            sampleSize = 500
            sampleSize = min(sampleSize, self.df.count())
            columnValuesSample = random.sample(self.df.select(colName).rdd.flatMap(lambda x: x).collect(), sampleSize)
            if (self.dtypes[colName] == "int") or (self.dtypes[colName] == "bigint"):
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
