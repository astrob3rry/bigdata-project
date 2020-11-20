# Milestones

## Project Choice

Our project is about spatial data profiling. We will profile 150 open data sets on spatial data which are selected from [NYC Open Data](https://opendata.cityofnewyork.us) using existing tools/libraries to identify their spatial attributes, and then try to come up with new techniques based on that to improve the precision and recall of the existing method.

## Previous Work and References

We searched some existing data profiling and cleaning tool that can be used as a starting point:

- [Datamart Profiler](https://pypi.org/project/datamart-profiler)
  - This library can profile datasets for use with Auctus, NYU’s Datamart system. You can use it to profile datasets on your side and send that to the server for search, instead of uploading the whole dataset.
- [Pandas Profiling](https://github.com/pandas-profiling/pandas-profiling)
  - This library generates profile reports from a pandas DataFrame.
- [Sweetviz](https://github.com/fbdesignpro/sweetviz)
  - This library generates beautiful, high-density visualizations to kickstart EDA (Exploratory Data Analysis).

In the mean time, we read some relevant papers regarding data profiling and cleaning as well:

- D. Loshin. 2010. The Practitioner's Guide to Data Quality Improvement (1st. ed.). Morgan Kaufmann Publishers Inc.

- Z. Abedjan, L. Golab, and F. Naumann. 2015. Profiling relational data: a survey. VLDB Journal.

- F. Naumann. 2013. Data Profiling Revisited, SIGMOD Record.
- Y. Y. Chiang, B. Wu, A. Anand, K. Akade, and C. A. Knoblock. 2014. A System for Efficient Cleaning and Transformation of Geospatial Data Attributes. Proceedings of the 22nd ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems.
- J. M. Hellerstein. 2008. Quantitative Data Cleaning for Large Databases. UC Berkeley.
- V. Ganti and A. D. Sarma. 2013. Data Cleaning: A Practical Perspective. Morgan & Claypool.
- E. Rahm and H. H. Do. 2000. Data Cleaning: Problems and Current Approaches. IEEE Data Engineering Bulletin.
- A. D. Chapman. 2005. Principles and Methods of Data Cleaning – Primary Species and Species-
  Occurrence Data, version 1.0. Report for the Global Biodiversity Information Facility.

- S. Wang and H. Yuan. 2014. Spatial Data Mining: A Perspective of Big Data. International Journal of Data Warehousing and Mining. 


## Problem Description and Goal

Open data often comes with little or no metadata, which makes it difficult to search for and find relevant datasets for a given information need. Therefore, the main goal for our project is to figure out a reliable and accurate way to provide informative summaries on spatial data for open datasets.

## Datasets

We have a sample size of 150, and all the datasets are selected from [NYC Open Data](https://opendata.cityofnewyork.us) with tabular structure and spatial data attributes.  

Check [datasets.md](https://github.com/astrob3rry/spatial-data-profiling/blob/main/datasets.md) in the same repository for more details.

## Proposed Method/Approach

// todo

## Evaluation Criteria
// todo

## Weekly Schedule

### Week 0: 2020.11.10 - 2020.11.15 ![done](https://progress-bar.dev/100/?title=done)

- :heavy_check_mark: Identify the problem that we want to work on and start searching relevant data sets. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry) 

- :heavy_check_mark: ​Read related literature in the field and look for spatial profiling tools that we can use in the project. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry) 

### Week 1: 2020.11.16 - 2020.11.22 ![ongoing](https://progress-bar.dev/40/?title=ongoing)

- Select 50 datasets that contain spatial data from NYC Open Data. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

- Try existing data profiling methods on a few datasets, and roughly measure the precision based on the self-defined metrics. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

- :heavy_check_mark: Ask professor following questions: [@Jiangfeng Lin](https://github.com/astrob3rry) 
  - We need to manually inspect all the datasets first, but how do we decide what to inspect? Are the information listed on the project description enough?

    <u>*We can decide what to inspect on our own.*</u>

  - How should we calculate precision and recall? What are these terms exactly?

    <u>*Based on the metrics we choose, and the inspection result we get, we should be able to calculate the precision for each tool.*</u>

  - Do we have to come up with a solution on our own? Is it fine that we just simply use another tool and make comparison?

    <u>*Simply comparing two tools isn’t enough. The main focus should be trying to improve the exisitng tool.*</u>

  - How do we incorporate Hadoop and Spark into our solution?

    <u>*We can set up several worker nodes, and have each of them processing a portion of datasets. However, we should take extra care of load balancing.*</u>
  
- :star: **Bonus task**: Find tools that can help identify data quality issues and generate data quality report. Also, look for tools that can geocode the location information for the datasets without latitude and longitude. [@Weili He](https://github.com/WeiliHe), [@Xianbo Gao](https://github.com/gaogxb), [@Jiangfeng Lin](https://github.com/astrob3rry)

### Week 2: 2020.11.23 - 2020.11.29

### Week 3: 2020.11.30 - 2020.12.06