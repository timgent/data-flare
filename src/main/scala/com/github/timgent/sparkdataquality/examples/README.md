# Example spark data quality usage
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

## Defining a set of data quality checks
In Example.scala there is some generic setup code