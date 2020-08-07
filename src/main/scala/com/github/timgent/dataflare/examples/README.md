# Example data flare usage
This example will take you through the main features of spark-data-quality, and give a short demonstration of what can
be achieved once you have some metric and quality check data stored in ElasticSearch.

We will cover:
1. Running a local version of ElasticSearch and Kibana so that we can store QC Results and metrics
2. Defining a set of data quality checks
3. Running those datachecks on a couple of datasets that change over time
4. Seeing the QC Results we get back at runtime
5. Reviewing QC Results in Kibana over time
6. Reviewing metrics in Kibana over time

Please clone this repository in order to run through the example.

## 1. Running ElasticSearch and Kibana
The simplest way to get up and running with ElasticSearch and Kibana is to use docker-compose. Please install this
from the instructions on the [docker website](https://docs.docker.com/compose/install/).

Once installed navigate to the examples directory in your terminal and run `docker-compose up`
```
cd spark-data-quality/src/main/scala/com/github/timgent/sparkdataquality/examples
docker-compose up
```

## 2. Defining a set of data quality checks
In Example.scala you will find the code for this example. In the Helpers object you will find the code responsible for
settings up the example checks.

### 3 & 4. Running checks on data that changes over time, and checking the QC Results
There are objects defining some sample data - Day1Data, Day2Data, etc.

Then there are runnable objects that use a local spark driver to perform checks - Day1Checks, Day2Checks, etc. Run
these objects to run the quality checks. If you'd like to check out the output returned either use some println
statements or use a debugger.

### 5 & 6. Load up Kibana
Use Kibana to do some visualizations. Using docker-compose it will be available at http://localhost:5601/

We have some pre-setup settings that you can use to view some basic data. In Kibana navigate to Management > Saved 
Objects. Select Import, and import the export.json file in the examples directory. 