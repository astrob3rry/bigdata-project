import pandas as pd
import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()

class DefaultFiles:
    def __init__(self, inputPath):
        self.dfCountryNames = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(os.path.join(inputPath, "countryNames.csv"))
        self.dfStateNames = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(os.path.join(inputPath, "stateNames.csv"))
        self.dfCityNames = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(os.path.join(inputPath, "cityNames.csv"))
        self.dfCountyNames = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(os.path.join(inputPath, "countyNames.csv"))
        self.dfNames = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(os.path.join(inputPath, "metadata.csv"))