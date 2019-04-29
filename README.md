# New York Taxi Data - Handling Big Data in Hadoop Ecosystem

This repo was created to share the codes and txt scripts used to:

- Create databases in MySQL to store historical data from taxi trips in NYC.
- Sqoop the data stored in MySQL to a NoSQL database, i.e. HBase.
- Using MapReduce framework to perform parallel and distributed processing given data stored in HBase.

For the MapReduce tasks there are two different sets of jobs which include: Driver, Mapper and Reducer written in Java:

(i) Querying data stored within HBase and writing the results in a new table also in HBase.

(ii) Querying data stored within HBase (referring to the results tables) and writing the results in HDFS to allow easy download.

Data specificatiom for this project:

NYC taxi trips made by both yellow and green taxi fleets between January 2017 to May 2017. Publicly available at: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
