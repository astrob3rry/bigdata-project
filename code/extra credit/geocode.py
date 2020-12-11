import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
        .builder \
        .appName("geocode") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

metadataPath = sys.argv[1]
metadataDF = spark.read.json(metadataPath, multiLine = True)

uscitiesDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])
cityLatLongDF = uscitiesDF.select("city", "lat", "lng").dropDuplicates()
cityLatLongDF = cityLatLongDF.withColumnRenamed("lat", "latitude")
cityLatLongDF = cityLatLongDF.withColumnRenamed("lng", "longitude")
cityLatLongDF = cityLatLongDF.withColumnRenamed("city", "geocode_city")
# cityLatLongDF = cityLatLongDF.withColumn("city",f.lower(f.col("city")))
# cityLatLongDF = cityLatLongDF.withColumn("city",f.trim(f.col("city")))
cityLatLongDF.createOrReplaceTempView("cities")
# cityLatLongDF.show()

uszipsDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[3])
zipLatLongDF = uszipsDF.select("zip", "lat", "lng").dropDuplicates()
zipLatLongDF = zipLatLongDF.withColumnRenamed("lat", "latitude")
zipLatLongDF = zipLatLongDF.withColumnRenamed("lng", "longitude")
zipLatLongDF = zipLatLongDF.withColumnRenamed("zip", "geocode_zip")
# zipLatLongDF = zipLatLongDF.withColumn("zip",f.lower(f.col("zip")))
# zipLatLongDF = zipLatLongDF.withColumn("zip",f.trim(f.col("zip")))
zipLatLongDF.createOrReplaceTempView("zips")
# zipLatLongDF.show()

fileNames = metadataDF.columns
for fileName in fileNames:
    print("***********current file name: {}***********".format(fileName))
    hdfsPath = "data/" + fileName
    fileSparkDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(hdfsPath)
    # fileSparkDF.createOrReplaceTempView("dataset")
    # print("current schema: {}".format(fileSparkDF.printSchema()))

    fileQueryStr = "`{}`.attributes.*".format(fileName)
    currMetadataDF = metadataDF.select(fileQueryStr)
    # currMetadataDF.show()

    currColumns = fileSparkDF.columns
    hasLatLong = False
    hasZipcode = False
    zipcodeIndex = 0
    hasBorough = False
    boroughIndex = 0
    hasCity = False
    cityIndex= 0
    for i in range(len(currColumns)):
        columnQueryStr = "`{}`.*".format(currMetadataDF.columns[i])
        colMetadataDF = currMetadataDF.select(columnQueryStr)
        if colMetadataDF.select("is_spatial").first()[0]:
            currType = colMetadataDF.select("type").first()[0]
            currIndex = colMetadataDF.select("index").first()[0]
            if currType == "latitude" or currType == "longitude":
                hasLatLong = True
                break
            elif currType == "zipcode":
                hasZipcode = True
                zipcodeIndex = currIndex
            elif currType == "borough":
                hasBorough = True
                boroughIndex = currIndex
            elif currType == "city":
                hasCity = True
                cityIndex = currIndex

    if hasLatLong:
        continue 
    else:
        if hasZipcode: 
            zipcodeColName = currColumns[zipcodeIndex]
            # fileSparkDF = fileSparkDF.withColumn(zipcodeColName,f.lower(f.col(zipcodeColName)))
            # fileSparkDF = fileSparkDF.withColumn(zipcodeColName,f.trim(f.col(zipcodeColName)))
            fileSparkDF.createOrReplaceTempView("currfile")
            fileLatLongDF = spark.sql("select * \
                                       from currfile left outer join zips \
                                       on trim(currfile.`{}`) = trim(zips.geocode_zip)".format(zipcodeColName))
            fileLatLongDF = fileLatLongDF.drop("geocode_zip")
            print("geocode has succeeded with the zipcode information.")
            fileLatLongDF.write.csv("geocode/{}".format(fileName[:-4]), header=True)
            continue
        if hasBorough:
            boroughColName = currColumns[boroughIndex]
            # fileSparkDF = fileSparkDF.withColumn(boroughColName,f.lower(f.col(boroughColName)))
            # fileSparkDF = fileSparkDF.withColumn(boroughColName,f.trim(f.col(boroughColName)))
            fileSparkDF.createOrReplaceTempView("currfile")
            fileLatLongDF = spark.sql("select * \
                                       from currfile left outer join cities \
                                       on trim(lower(currfile.`{}`)) = trim(lower(cities.geocode_city))".format(boroughColName))
            fileLatLongDF = fileLatLongDF.drop("geocode_city")
            print("geocode has succeeded with the borough information.")
            fileLatLongDF.write.csv("geocode/{}".format(fileName[:-4]), header=True)
            continue
        if hasCity:
            cityColName = currColumns[cityIndex]
            # fileSparkDF = fileSparkDF.withColumn(cityColName,f.lower(f.col(cityColName)))
            # fileSparkDF = fileSparkDF.withColumn(cityColName,f.trim(f.col(cityColName)))
            fileSparkDF.createOrReplaceTempView("currfile")
            fileLatLongDF = spark.sql("select * \
                                       from currfile left outer join cities \
                                       on trim(lower(currfile.`{}`)) = trim(lower(cities.geocode_city))".format(cityColName))
            fileLatLongDF = fileLatLongDF.drop("geocode_city")
            print("geocode has succeeded with the city information.")
            fileLatLongDF.write.csv("geocode/{}".format(fileName[:-4]), header=True)

spark.stop()