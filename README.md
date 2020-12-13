# Spatial Data Profiling

## Description

In reality, open data repositories usually do not provide rich summaries about spatial data. Take NYC Open Data for example, it labels the datasets in a coarse way. There are different categories based on the industries, and different data types, agencies, and tags. However, none of these metadata offers much help for the data consumers to quickly identify the spatial data that they actually need. 

Based on this context, Therefore, the problem that this project aims to automate the process of spatial data attributes detection, therefore being able to provide informative spatial metadata about any input open datasets. Our solution should also be scalable, so it can process a large volume of datasets in one go. In particular, our focus of the data source is [NYC Open Data](https://opendata.cityofnewyork.us). 

## Data

We put our 150 datasets selected from NYC Open Data in the [Google Drive](https://drive.google.com/drive/folders/1M1I3q3wtPH0RBaR4RiTcopyxZxc6cQhv?usp=sharing), and all the other data that we used in the project can be found in the `data/` folder.

- `cityNames.csv` lists out all the city names in the US.
- `countryNames.csv` lists out all the country names in the world.
- `countyNames.csv` lists out all the county names in the US.
- `dataset.md` lists out the detailed dataset information, including its original link.
- `metadata.csv` and `matadata.json` are all the manual inspection result of the 150 datasets.
- `stateNames.csv` lists out all the state names in the US.
- `uscities.csv` also lists out all the cities in the US, but it also includes some other supplemental information such as state, latitude, longitude, population, etc.
- `uscities.csv` lists out all the zipcodes in the US, and it also includes latitude, longitude, and the city this zipcode falls into.

## Installation

We recommend you install the following packages before you run the code:

- [Datamart Geo](https://pypi.org/project/datamart-geo)

- [Datamart Profiler](https://pypi.org/project/datamart-profiler/) 
- [Pandas](https://github.com/pandas-dev/pandas)

- [Spark](https://github.com/apache/spark)

- [FuzzyWuzzy](https://github.com/seatgeek/fuzzywuzzy) 
- [PyDeequ](https://github.com/awslabs/python-deequ)

## How to Run

### Original Method

//todo

### Improved Method
To Run in HDSF, follow the below steps

1. Put all the datasets in the `data` folder, store `countryNames.csv`, `stateNames.csv`, `cityNames.csv`, `countyNames.csv`, `metadata.csv` into the `helper` folder for reading as default helper files. Also upload the mmh3.py and snapy.zip to remote computer

2. Run the automactically identification procedure `Run.py`. Specify the datasets folder, helper folder, output folder as sys.argv[1], sys.argv[2], sys.argv[3], run the below command 
```shell
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files /home/path~/mmh3.py --py-files /home/path~/snapy.zip /path~/Run.py /user/path~/data /user/path~/helper /user/path~/out
```
Then we can output two files `result.json` and `dfNameColNamesDict.json`. `result.json` is the automactically identification result and `dfNameColNamesDict.json` is storing the column names for each dataset file name

3. Calculate the accuracy using `result.json` and `metadata.json` (benchmark result). Specify the output file path as sys.argv[1], helper folder as sys.argv[2], output folder as sys.argv[3], run the below command.
```shell
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --py-files /home/path~/mmh3.py --py-files /home/path~/snapy.zip RunAccuracy.py /user/path~/out /user/path~/helper /user/path~/out2
```
Then we can output two files `accuracy.json` which stores the TP (true positive), FP, FN, TN overall and for each spatial type and `comparison.json` which stores the difference between benchmark and automactically detection result.
### Exploration Work

#### Quality Analysis

1. Download all the datasets in the Google Drive, together with `metadata.json`, `uscities.csv`, and `uszips.csv` in the `data` folder. 

2. Store all these files in HDFS. In our case, we store all the datasets in the `data` directory, while all the other metadata in the root directory under our account folder.

3. If you do not have the authority to install packages in the cluster, you could download the jar file and the source code for `pydeequ` that we put in the `lib` folder, and then pass them as arguments when you submit your spark job. 

```shell
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --jars deequ-1.0.5.jar --py-files pydeequ.zip quality_analysis.py metadata.json uscities.csv uszips.csv
```

#### Geocode Automation

1. Download all the datasets in the Google Drive, together with `metadata.json`, `uscities.csv`, and `uszips.csv` in the `data` folder. 

2. Store all these files in HDFS. In our case, we store all the datasets in the `data` directory, while all the other metadata in the root directory under our account folder.

3. Submit your spark job.

```shell
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python geocode.py metadata.json uscities.csv uszips.csv
```

## Contributors

- [Weili He](https://github.com/WeiliHe)
- [Xianbo Gao](https://github.com/gaogxb)
- [Jiangfeng Lin](https://github.com/astrob3rry)

November 10, 2020 - December 14, 2020 :cupid:
