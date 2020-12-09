import sys
import json

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pydeequ
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *

spark = SparkSession \
        .builder \
        .appName("quality-analysis") \
        .config("spark.some.config.option", "some-value") \
        .config("spark.driver.extraClassPath", "./") \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()
spark.sparkContext.addPyFile("pydeequ.zip")

### put "metadata.json" in the spark-submit argument
metadataPath = sys.argv[1]
# print("path is: {}".format(path))

### read "metadata.json" into a spark DF
metadataDF = spark.read.json(metadataPath, multiLine = True)
# metadataDF.printSchema()

uscitiesDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])
uszipsDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[3])

cityNameDF = uscitiesDF.select("city").dropDuplicates()
cityNameDF = cityNameDF.withColumn("city",f.lower(f.col("city")))
cityNameDF = cityNameDF.withColumn("city",f.trim(f.col("city")))
cityNameDF.createOrReplaceTempView("cities")
stateAbbrDF = uscitiesDF.select("state_id").dropDuplicates()
stateAbbrDF = stateAbbrDF.withColumn("state_id",f.lower(f.col("state_id")))
stateAbbrDF = stateAbbrDF.withColumn("state_id",f.trim(f.col("state_id")))
stateAbbrDF.createOrReplaceTempView("state_ids")
stateNameDF = uscitiesDF.select("state_name").dropDuplicates()
stateNameDF = stateNameDF.withColumn("state_name",f.lower(f.col("state_name")))
stateNameDF = stateNameDF.withColumn("state_name",f.trim(f.col("state_name")))
stateNameDF.createOrReplaceTempView("state_names")
zipDF = uszipsDF.select("zip").dropDuplicates()
zipDF = zipDF.withColumn("zip",f.lower(f.col("zip")))
zipDF = zipDF.withColumn("zip",f.trim(f.col("zip")))
zipDF.createOrReplaceTempView("zips")
# uscitiesDF.printSchema()
# uszipsDF.printSchema()
# cityNameDF.show()
# stateAbbrDF.show()
# stateNameDF.show()
# zipDF.show()

### get a list of file names from the spark DF
fileNames = metadataDF.columns
# print ("file names type: {}".format(type(fileNames)))
# print("fileNames: {}".format(fileNames))

### read each file into a spark DF, and process them one by one accordingly

boroSize = 0
numBoroAttrs = 0
boroCompleteness = 0
boroDistinct = 0
numValid = 0
for fileName in fileNames:
    print("***********current file name: {}***********".format(fileName))
    hdfsPath = "data/" + fileName
    fileSparkDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(hdfsPath)
    fileSparkDF.createOrReplaceTempView("dataset")
    # print("current schema: {}".format(fileSparkDF.printSchema()))

    fileQueryStr = "`{}`.attributes.*".format(fileName)
    currMetadataDF = metadataDF.select(fileQueryStr)
    # currMetadataDF.show()

    currColumns = fileSparkDF.columns
    for i in range(len(currColumns)):
        # print("current column name: {}".format(currColumns[i]))
        columnQueryStr = "`{}`.*".format(currMetadataDF.columns[i])
        colMetadataDF = currMetadataDF.select(columnQueryStr)
        # colMetadataDF.show()
        if colMetadataDF.select("is_spatial").first()[0]:
            currType = colMetadataDF.select("type").first()[0]
            currIndex = colMetadataDF.select("index").first()[0]
            if currType == "borough":
                # get the dataframe, union together with other dfs
                # do a quality analysis after
                # add the data together

                # print("***********currrent borough column name: {}***********".format(currColumns[currIndex]))
                boroughColName = currColumns[currIndex]
                boroughDF = spark.sql("select `{}` as boro from dataset".format(boroughColName)) 
                boroughDF = boroughDF.withColumn("boro",f.lower(f.col("boro")))
                boroughDF = boroughDF.withColumn("boro",f.trim(f.col("boro")))
                boroughDF.createOrReplaceTempView("currboro")
                # boroughDF.show()

                analysisResult = AnalysisRunner(spark) \
                    .onData(boroughDF) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("boro")) \
                    .addAnalyzer(CountDistinct("boro")) \
                    .run()
                analysisResultDF = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
                boroSize += analysisResultDF.where(analysisResultDF.name == "Size").select("value").first()[0]
                boroCompleteness += analysisResultDF.where(analysisResultDF.name == "Completeness").select("value").first()[0]
                boroDistinct += analysisResultDF.where(analysisResultDF.name == "CountDistinct").select("value").first()[0]
                numBoroAttrs += 1

                validBoroughDF = spark.sql("select currboro.boro \
                                            from currboro \
                                            where exists (select * \
                                                          from cities \
                                                          where currboro.boro = cities.city)")
                # validBoroughDF.show()
                print("num valid borough: {}".format(validBoroughDF.count()))
                print("total size: {}".format(boroSize))
                numValid += validBoroughDF.count()

            # elif currType == "city":
            #     # print("city column name: {}".format(currColumns[currIndex]))
            #     cityColName = currColumns[currIndex]
            #     cityDF = spark.sql("select `{}` as city from dataset".format(cityColName)) 
            #     cityDF.show()
            # elif currType == "state":
            #     # print("state column name: {}".format(currColumns[currIndex]))
            #     stateColName = currColumns[currIndex]
            #     stateDF = spark.sql("select `{}` as state from dataset".format(stateColName))
            #     stateDF.show()
            # elif currType == "zipcode":
            #     # print("zipcode column name: {}".format(currColumns[currIndex]))
            #     zipcodeColName = currColumns[currIndex]
            #     zipcodeDF = spark.sql("select `{}` as zipcode from dataset".format(zipcodeColName))
            #     zipcodeDF.show()
    
    # qualityAnaDF = spark.sql("select {} as borough, {} as city, {} as state, {} as zipcode \
                            #   from dataset".format(boroughColName, cityColName, stateColName, zipcodeColName))

print("total num of boro: {}".format(numBoroAttrs))
print("avg boro size: {}".format(boroSize / numBoroAttrs))
print("avg boro completeness: {}".format(boroCompleteness / numBoroAttrs))
print("avg boro count distinct: {}".format(boroDistinct / numBoroAttrs))

spark.stop()