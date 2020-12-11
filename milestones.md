# Milestones

## Project Choice

Our project is about spatial data profiling for [NYC Open Data](https://opendata.cityofnewyork.us). We will profile 150 open data sets on spatial data which are selected from [NYC Open Data](https://opendata.cityofnewyork.us) using existing tools/libraries such as [Datamart Profiler](https://pypi.org/project/datamart-profiler) to identify their spatial attributes, and then try to come up with new techniques based on that to improve the precision and recall of the existing method.

## Previous Work and References

We searched some existing data profiling and cleaning tool that can be used as a starting point:

- [Datamart Profiler](https://pypi.org/project/datamart-profiler)
  - This library can profile datasets for use with Auctus, NYU’s Datamart system. You can use it to profile datasets on your side and send that to the server for search, instead of uploading the whole dataset.
- [Pandas profiling](https://github.com/pandas-profiling/pandas-profiling)
  - Create HTML profiling reports from pandas DataFrame objects.
- [sweetviz](https://github.com/fbdesignpro/sweetviz)
  - Visualize and compare datasets, target values and associations, with one line of code.
- [FuzzyWuzzy](https://github.com/seatgeek/fuzzywuzzy)
  - FuzzyWuzzy uses [Levenshtein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) to calculate the differences between sequences in a simple-to-use package.
- [usaddress](https://github.com/datamade/usaddress)
  - usaddress is a Python library for parsing unstructured address strings into address components, using advanced NLP methods.
- [UPS Developer Kit](https://www.ups.com/upsdeveloperkit?loc=en_US)
  - UPS Developer Kit contains address validation APIs for City, State, ZIP, and stress address.
- [Google Maps Platform](https://developers.google.com/maps/gmp-get-started)
  - The APIs about what you may want to do on a map or with location-based data.
- [OpenRefine](https://openrefine.org/)
  - OpenRefine (previously Google Refine) is a powerful tool for working with messy data:  cleaning it; transforming it from one format into another; and extending it with  web services and external data.
- [geopy](https://github.com/geopy/geopy)
  - geopy is a Python client for several popular geocoding web services.
- [pygeocoder](https://pypi.org/project/pygeocoder/)
  - geocoder allows you to directly convert an address to coordinates or vice  versa. It also allows you to validate, format and split addresses into  civic number, street name, city etc.

In the mean time, we read some relevant papers regarding spatial data profiling and cleaning as well:

- Z. Abedjan, L. Golab, and F. Naumann. 2015. Profiling relational data: a survey. VLDB Journal.
- F. Naumann. 2013. Data Profiling Revisited, SIGMOD Record.
- D. Loshin. 2010. The Practitioner's Guide to Data Quality Improvement (1st. ed.). Morgan Kaufmann Publishers Inc.
- V. Ganti and A. D. Sarma. 2013. Data Cleaning: A Practical Perspective. Morgan & Claypool.
- E. Rahm and H. H. Do. 2000. Data Cleaning: Problems and Current Approaches. IEEE Data Engineering Bulletin.
- A. D. Chapman. 2005. Principles and Methods of Data Cleaning – Primary Species and Species-
  Occurrence Data, version 1.0. Report for the Global Biodiversity Information Facility.


## Problem Description and Goal

Open data often comes with little or no metadata, which makes it difficult to search for and find relevant datasets for a given information need. Based on this context, the main goal for our project is to figure out a relatively reliable and accurate way to provide informative summaries on spatial data for the datasets from [NYC Open Data](https://opendata.cityofnewyork.us), and explore the difficulties that we encounter while profiling the spatial data. Especially, the focus of the project will be spatial attributes detection.

## Datasets

We have a sample size of 150, and all the datasets are selected from [NYC Open Data](https://opendata.cityofnewyork.us) with tabular structure and spatial data attributes.  

Check [datasets.md](https://github.com/astrob3rry/spatial-data-profiling/blob/main/datasets.md) in the same repository for more details.

## Proposed Method/Approach

- Based on the datasets that we collected, we will first manually inspect each dataset to obtain the metadata of the spatial information. For example, how many location-related attributes it has, and what are they? Does it contain information about latitude and longitude? etc.  

- We will then use [Datamart Profiler](https://pypi.org/project/datamart-profiler) as our starting point, and perform the detection of spatial attributes such as latitude and longitude, and report the precision and recall accordingly.
- Based on the results, we will come up with improvements and evaluate the new method. A basic idea is that we could collect all the frequently-used spatial data attributes names, and do a fuzzy string matching using [FuzzyWuzzy](https://github.com/seatgeek/fuzzywuzzy) and regular expression to do the spatial data detection. If necessary and time allows, we could also introduce machine learning to do a more fine-grained classification.
- We will also come up with a way to divide the whole sample of datasets into different jobs, and make them running concurrently in Spark cluster.
- For data quality issues such as incorrect values and missing data that we learned while profiling the data, we would probably use some Web APIs such as [UPS Developer Kit](https://www.ups.com/upsdeveloperkit?loc=en_US), and also some open-source tools such as [OpenRefine](https://openrefine.org/) to help us generate a quality report to learn more about the details. For the dataset that does not have information about latitude and longitude, we could use some libraries such [geopy](https://github.com/geopy/geopy) to geocode the location information, and create new columns with the latitude and longitude information.

## Evaluation Criteria
We will manually inspect the datasets to obtain the metadata about the spatial data, which will be used as the metrics to measure the precision of the proposed method and the improved approach.  

Check [metadata.csv](https://github.com/astrob3rry/spatial-data-profiling/blob/main/metadata.csv) in the same repository for more details.

## Weekly Schedule

### Week 0: 2020.11.10 - 2020.11.15 ![done](https://progress-bar.dev/100/?title=done)

- :heavy_check_mark: Identify the problem that we want to work on and start searching relevant data sets. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry) 

- :heavy_check_mark: ​Read related literature in the field and look for spatial profiling tools that we can use in the project. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry) 

### Week 1: 2020.11.16 - 2020.11.22 ![done](https://progress-bar.dev/100/?title=done)

-  :heavy_check_mark: Select 150 datasets that contain spatial data from NYC Open Data. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

- :heavy_check_mark:  Try existing data profiling methods on a few datasets, and roughly measure the precision based on the self-defined metrics. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

  - <u>*We’ve tried both Datamart profiler and Pandas Profiling, and decided to go with Datamart Profiler.*</u>

- :heavy_check_mark: Ask professor following questions: [@Jiangfeng Lin](https://github.com/astrob3rry) 
  - We need to manually inspect all the datasets first, but how do we decide what to inspect? Are the information listed on the project description enough?

    <u>*We can decide what to inspect on our own.*</u>

  - How should we calculate precision and recall? What are these terms exactly?

    <u>*Based on the metrics we choose, and the inspection result we get, we should be able to calculate the precision for each tool.*</u>

  - Do we have to come up with a solution on our own? Is it fine that we just simply use another tool and make comparison?

    <u>*Simply comparing two tools isn’t enough. The main focus should be trying to improve the exisitng tool.*</u>

  - How do we incorporate Hadoop and Spark into our solution?

    <u>*We can set up several worker nodes, and have each of them processing a portion of datasets. However, we should take extra care of load balancing.*</u>
  
- :heavy_check_mark: :star: **Bonus task**: Find tools that can help identify data quality issues and generate data quality report. Also, look for tools that can geocode the location information for the datasets without latitude and longitude. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

  - <u>*Data quality issues: OpenRefine, usaddress, Web APIs such as UPS Developer Kit.*</u>
  - <u>*Geocode: geopy, pygeocoder.*</u>

### Week 2: 2020.11.23 - 2020.11.29 ![done](https://progress-bar.dev/80/?title=unfinished)

- :heavy_check_mark: Manually inspect 150 datasets, and summarize the corresponding result into a csv file. [@Jiangfeng Lin](https://github.com/astrob3rry)
- :warning: Learn how to run multiple jobs simultaneously in Spark. [@Jiangfeng Lin](https://github.com/astrob3rry)
  - <u>*Jiangfeng has been looked into several ways of incorporating Spark, like trying to read in multiple files into one RDD using wholeTextFiles, and also using shell script to execute spark-submit commands several times. However, none of those worked elegantly.  After the discussion, Weili and Xianbo both thought we should do multi-threading and manipulate multiple RDDs in one sitting.*</u>
- :heavy_check_mark: Use Datamart profiler to perform spatial data profiling, and improve this method after identifying the limitations. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb)
  - <u>*We noticed that Datamart profiler could only reliably identify “latitude” and “longitude”. We decided to use fuzzy match and regular expression to help improve the method.*</u>

### Week 3: 2020.11.30 - 2020.12.06 ![ongoing](https://progress-bar.dev/70/?title=unfinished)

#### :runner: Sprint 1: 2020.11.30 - 2020.12.02

- :heavy_check_mark: ​Upload all the files to the Dumbo cluster, and create the target JSON file for accuracy comparison. [@Jiangfeng Lin](https://github.com/astrob3rry)
- :heavy_check_mark: Improve the current code for spatial data detection. [@Weili He](https://github.com/WeiliHe)
- :warning: Come up with another idea of how to implement Spark, and try to run the code on the Dumbo cluster. [@Xianbo Gao](https://github.com/gaogxb)
  - <u>*Didn’t get a chance to do this because of deadline conflicts.*</u>

#### :runner: ​Sprint 2: 2020.12.03 - 2020.12.06

- :warning: Deploy our current code using Spark. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)
  - <u>*Since our project actually doesn’t perfectly fit into a map reduce programming model(we only have a map phase), it’s hard to refactor our current code using pyspark. Also, some libraries we are currently using are only compatible with pandas dataframe. This task is way harder than we have imagined it to be, therefore we couldn’t finish it on time.*</u>
-  :heavy_check_mark: Discuss when and why our improved profiling approach fails. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

### Week 4: 2020.12.07 - 2020.12.13 ![ongoing](https://progress-bar.dev/80/?title=ongoing)

#### :runner: Sprint 1: 2020.12.07 - 2020.12.11

- :heavy_check_mark: Calculate the accuracy of spatial column detection using Datamart. [@Xianbo Gao](https://github.com/gaogxb)
- :heavy_check_mark: Refactor the current python code to make it runnable under Spark. [@Weili He](https://github.com/WeiliHe)
- :heavy_check_mark: ​Identify quality issues and generate quality reports.  [@Jiangfeng Lin](https://github.com/astrob3rry)
- :heavy_check_mark: Geocode the location for the datasets that do not have columns for latitude and longitude.  [@Jiangfeng Lin](https://github.com/astrob3rry)
- :heavy_check_mark: Draft the final project report using LaTeX. [@Jiangfeng Lin](https://github.com/astrob3rry), [@Weili He](https://github.com/WeiliHe)
- :heavy_check_mark: Revise and improve the report.  [@Xianbo Gao](https://github.com/gaogxb)

#### :runner: Sprint 2: 2020.12.12 - 2020.12.13

- Make presentation slides.  [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)
- Prepare for the project presentation. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)
