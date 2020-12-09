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
stateAbbrDF = stateAbbrDF.withColumnRenamed("state_id", "state")
stateNameDF = uscitiesDF.select("state_name").dropDuplicates()
stateNameDF = stateNameDF.withColumn("state_name",f.lower(f.col("state_name")))
stateNameDF = stateNameDF.withColumn("state_name",f.trim(f.col("state_name")))
stateNameDF = stateNameDF.withColumnRenamed("state_name", "state")
stateDF = stateAbbrDF.union(stateNameDF)
stateDF.createOrReplaceTempView("states")
zipDF = uszipsDF.select("zip").dropDuplicates()
zipDF = zipDF.withColumn("zip",f.lower(f.col("zip")))
zipDF = zipDF.withColumn("zip",f.trim(f.col("zip")))
zipDF.createOrReplaceTempView("zips")
# uscitiesDF.printSchema()
# uszipsDF.printSchema()
# cityNameDF.show()
# stateAbbrDF.show()
# stateNameDF.show()
# stateDF.show()
# zipDF.show()

### get a list of file names from the spark DF
fileNames = metadataDF.columns
# print ("file names type: {}".format(type(fileNames)))
# print("fileNames: {}".format(fileNames))

### read each file into a spark DF, and process them one by one accordingly

boroSize = 0
numBoroAttrs = 0
boroNonNull = 0
boroDistinct = 0
boroValid = 0

citySize = 0
numCityAttrs = 0
cityNonNull = 0
cityDistinct = 0
cityValid = 0

stateSize = 0
numStateAttrs = 0
stateNonNull = 0
stateDistinct = 0
stateValid = 0

zipcodeSize = 0
numZipcodeAttrs = 0
zipcodeNonNull = 0
zipcodeDistinct = 0
zipcodeValid = 0

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
                boroCurrSize = analysisResultDF.where(analysisResultDF.name == "Size").select("value").first()[0]
                boroCurrCompleteness = analysisResultDF.where(analysisResultDF.name == "Completeness").select("value").first()[0]
                boroSize += boroCurrSize
                boroNonNull += boroCurrSize * boroCurrCompleteness
                boroDistinct += analysisResultDF.where(analysisResultDF.name == "CountDistinct").select("value").first()[0]
                numBoroAttrs += 1

                validBoroughDF = spark.sql("select currboro.boro \
                                            from currboro \
                                            where exists (select * \
                                                          from cities \
                                                          where currboro.boro = cities.city)")
                # validBoroughDF.show()
                # print("num valid borough: {}".format(validBoroughDF.count()))
                # print("total size: {}".format(boroSize))
                boroValid += validBoroughDF.count()
            elif currType == "city":
                # print("city column name: {}".format(currColumns[currIndex]))
                cityColName = currColumns[currIndex]
                cityDF = spark.sql("select `{}` as city from dataset".format(cityColName)) 
                cityDF = cityDF.withColumn("city",f.lower(f.col("city")))
                cityDF = cityDF.withColumn("city",f.trim(f.col("city")))
                cityDF.createOrReplaceTempView("currcity")
                # cityDF.show()

                analysisResult = AnalysisRunner(spark) \
                    .onData(cityDF) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("city")) \
                    .addAnalyzer(CountDistinct("city")) \
                    .run()
                analysisResultDF = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
                cityCurrSize = analysisResultDF.where(analysisResultDF.name == "Size").select("value").first()[0]
                cityCurrCompleteness = analysisResultDF.where(analysisResultDF.name == "Completeness").select("value").first()[0]
                citySize += cityCurrSize
                cityNonNull += cityCurrSize * cityCurrCompleteness
                cityDistinct += analysisResultDF.where(analysisResultDF.name == "CountDistinct").select("value").first()[0]
                numCityAttrs += 1

                validCityDF = spark.sql("select currcity.city \
                                         from currcity \
                                         where exists (select * \
                                                       from cities \
                                                       where currcity.city = cities.city)")
                # validCityDF.show()
                # print("num valid city: {}".format(validCityDF.count()))
                # print("total size: {}".format(citySize))
                cityValid += validCityDF.count()
            elif currType == "state":
                # print("state column name: {}".format(currColumns[currIndex]))
                stateColName = currColumns[currIndex]
                stateDF = spark.sql("select `{}` as state from dataset".format(stateColName)) 
                stateDF = stateDF.withColumn("state",f.lower(f.col("state")))
                stateDF = stateDF.withColumn("state",f.trim(f.col("state")))
                stateDF.createOrReplaceTempView("currstate")
                # stateDF.show()

                analysisResult = AnalysisRunner(spark) \
                    .onData(stateDF) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("state")) \
                    .addAnalyzer(CountDistinct("state")) \
                    .run()
                analysisResultDF = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
                stateCurrSize = analysisResultDF.where(analysisResultDF.name == "Size").select("value").first()[0]
                stateCurrCompleteness = analysisResultDF.where(analysisResultDF.name == "Completeness").select("value").first()[0]
                stateSize += stateCurrSize
                stateNonNull += stateCurrSize * stateCurrCompleteness
                stateDistinct += analysisResultDF.where(analysisResultDF.name == "CountDistinct").select("value").first()[0]
                numStateAttrs += 1

                validStateDF = spark.sql("select currstate.state \
                                            from currstate \
                                            where exists (select * \
                                                          from states \
                                                          where currstate.state = states.state)")
                # validStateDF.show()
                # print("num valid state: {}".format(validStateDF.count()))
                # print("total size: {}".format(stateSize))
                stateValid += validStateDF.count()
            elif currType == "zipcode":
                # print("zipcode column name: {}".format(currColumns[currIndex]))
                zipcodeColName = currColumns[currIndex]
                zipcodeDF = spark.sql("select `{}` as zipcode from dataset".format(zipcodeColName)) 
                zipcodeDF = zipcodeDF.withColumn("zipcode",f.lower(f.col("zipcode")))
                zipcodeDF = zipcodeDF.withColumn("zipcode",f.trim(f.col("zipcode")))
                zipcodeDF.createOrReplaceTempView("currzipcode")
                # zipcodeDF.show()

                analysisResult = AnalysisRunner(spark) \
                    .onData(zipcodeDF) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("zipcode")) \
                    .addAnalyzer(CountDistinct("zipcode")) \
                    .run()
                analysisResultDF = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
                zipcodeCurrSize = analysisResultDF.where(analysisResultDF.name == "Size").select("value").first()[0]
                zipcodeCurrCompleteness = analysisResultDF.where(analysisResultDF.name == "Completeness").select("value").first()[0]
                zipcodeSize += zipcodeCurrSize
                zipcodeNonNull += zipcodeCurrSize * zipcodeCurrCompleteness
                zipcodeDistinct += analysisResultDF.where(analysisResultDF.name == "CountDistinct").select("value").first()[0]
                numZipcodeAttrs += 1

                validZipcodeDF = spark.sql("select currzipcode.zipcode \
                                            from currzipcode \
                                            where exists (select * \
                                                          from zips \
                                                          where currzipcode.zipcode = zips.zip)")
                # validZipcodeDF.show()
                # print("num valid zipcode: {}".format(validZipcodeDF.count()))
                # print("total size: {}".format(zipcodeSize))
                zipcodeValid += validZipcodeDF.count()
    

print("total lines of boro records: {}".format(boroSize))
print("total lines of non-null boro records: {}".format(boroNonNull))
print("total occurance time of boro columns: {}".format(numBoroAttrs))
print("avg boro records in each occurance: {}".format(boroSize / numBoroAttrs))
print("avg boro completeness(percentage of non-null values): {}".format(boroNonNull / boroSize))
print("total boro distinct values: {}".format(boroDistinct))
print("avg distinctive values in each boro columns: {}".format(boroDistinct / numBoroAttrs))
print("distinctiveness of boro: {}".format(boroDistinct / boroNonNull))
print("total valid boro records: {}".format(boroValid))
print("avg boro data correctness: {}".format(boroValid / boroNonNull))

print("total lines of city records: {}".format(citySize))
print("total lines of non-null city records: {}".format(cityNonNull))
print("total occurance time of city columns: {}".format(numCityAttrs))
print("avg city records in each occurance: {}".format(citySize / numCityAttrs))
print("avg city completeness(percentage of non-null values): {}".format(cityNonNull / citySize))
print("total city distinct values: {}".format(cityDistinct))
print("avg distinctive values in each city columns: {}".format(cityDistinct / numCityAttrs))
print("distinctiveness of city: {}".format(cityDistinct / cityNonNull))
print("total valid city records: {}".format(cityValid))
print("avg city data correctness: {}".format(cityValid / cityNonNull))

print("total lines of state records: {}".format(stateSize))
print("total lines of non-null state records: {}".format(stateNonNull))
print("total occurance time of state columns: {}".format(numStateAttrs))
print("avg state records in each occurance: {}".format(stateSize / numStateAttrs))
print("avg state completeness(percentage of non-null values): {}".format(stateNonNull / stateSize))
print("total state distinct values: {}".format(stateDistinct))
print("avg distinctive values in each state columns: {}".format(stateDistinct / numStateAttrs))
print("distinctiveness of state: {}".format(stateDistinct / stateNonNull))
print("total valid state records: {}".format(stateValid))
print("avg state data correctness: {}".format(stateValid / stateNonNull))

print("total lines of zipcode records: {}".format(zipcodeSize))
print("total lines of non-null zipcode records: {}".format(zipcodeNonNull))
print("total occurance time of zipcode columns: {}".format(numZipcodeAttrs))
print("avg zipcode records in each occurance: {}".format(zipcodeSize / numZipcodeAttrs))
print("avg zipcode completeness(percentage of non-null values): {}".format(zipcodeNonNull / zipcodeSize))
print("total zipcode distinct values: {}".format(zipcodeDistinct))
print("avg distinctive values in each zipcode columns: {}".format(zipcodeDistinct / numZipcodeAttrs))
print("distinctiveness of zipcode: {}".format(zipcodeDistinct / zipcodeNonNull))
print("total valid zipcode records: {}".format(zipcodeValid))
print("avg zipcode data correctness: {}".format(zipcodeValid / zipcodeNonNull))

spark.stop()