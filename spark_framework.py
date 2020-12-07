'''
module load python/gnu/3.4.4
module load spark/2.4.0
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python spark_framework.py metadata.json
'''

import sys
import json
import random
import re
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
# from snapy import MinHash, LSH

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# test fuzzywuzzy
print("ratio: {}".format(fuzz.ratio("this is a test", "this is a test!")))
choices = ["Atlanta Falcons", "New York Jets", "New York Giants", "Dallas Cowboys"]
process.extract("new york jets", choices, limit=2)
print("process: {}".format(process.extract("new york jets", choices, limit=2)))

spark = SparkSession.builder.appName("project-test").config("spark.some.config.option", "some-value").getOrCreate()
# spark.sparkContext.addPyFile("mmh3.zip")
spark.sparkContext.addPyFile("mmh3.py")
spark.sparkContext.addPyFile("snapy.zip")

### test snapy: not working because of mmh3
import mmh3
from snapy import MinHash, LSH
content = [
    'Jupiter is primarily composed of hydrogen with a quarter of its mass '
    'being helium',
    'Jupiter moving out of the inner Solar System would have allowed the '
    'formation of inner planets.',
    'A helium atom has about four times as much mass as a hydrogen atom, so '
    'the composition changes when described as the proportion of mass '
    'contributed by different atoms.',
    'Jupiter is primarily composed of hydrogen and a quarter of its mass '
    'being helium',
    'A helium atom has about four times as much mass as a hydrogen atom and '
    'the composition changes when described as a proportion of mass '
    'contributed by different atoms.',
    'Theoretical models indicate that if Jupiter had much more mass than it '
    'does at present, it would shrink.',
    'This process causes Jupiter to shrink by about 2 cm each year.',
    'Jupiter is mostly composed of hydrogen with a quarter of its mass '
    'being helium',
    'The Great Red Spot is large enough to accommodate Earth within its '
    'boundaries.'
]
labels = [1, 2, 3, 4, 5, 6, 7, 8, 9]
seed = 3
# Create MinHash object.
testMinhash = MinHash(content, n_gram=9, permutations=100, hash_bits=64, seed=3)
# Create LSH model.
testLsh = LSH(testMinhash, labels, no_of_bands=50)
# Query to find near duplicates for text 1.
print("snapy result: {}".format(testLsh.query(1, min_jaccard=0.5)))


# org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 18 tasks (1052.8 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
# spark.conf.set("spark.driver.maxResultSize", "10g")
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")

### put "metadata.json" in the spark-submit argument
path = sys.argv[1]
# print("path is: {}".format(path))

### read "metadata.json" into a spark DF
metadataDF = spark.read.json(path, multiLine = True)
# metadataDF.printSchema()

### get a list of file names from the spark DF
fileNames = metadataDF.columns
# print ("file names type: {}".format(type(fileNames)))
# print("fileNames: {}".format(fileNames))

### read each file into a spark DF, and process them one by one accordingly
for fileName in fileNames:
    print("***********current file name: {}***********".format(fileName))
    hdfsPath = "data/" + fileName
    fileSparkDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(hdfsPath)
    # print("current schema: {}".format(fileSparkDF.printSchema()))

    columns = fileSparkDF.columns
    for col in columns:
        print("current column name: {}".format(col))

    ### turn spark DF to pandas DF(does not work if the file is too large)
    # filePandasDF = fileSparkDF.toPandas()
    ### output the columns
    # colNames = filePandasDF.columns.values
    # for col in colNames:
    #     print("current column name: {}".format(col))
    ### create a spark DF from a pandas DF
    # ResultSparkDF = spark.createDataFrame(filePandasDF)

spark.stop()
