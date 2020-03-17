# udacity capstone project

## Project and Data Introduction
This is my udacity capstone project. Using Udacity Provided datasets and AWS EMR for data preprocessing, and save to parquet file in S3. this project's data pipeline is build by Airflow. The deployment and infrastructure of this project uses AWS cloudformation.

Below are datasets used in this project:
### I94 Immigration Data: 
This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.
https://travel.trade.gov/research/reports/i94/historical/2016.html

### U.S. City Demographic Data:
This data comes from OpenSoft.
https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

### Airport Code Table:

This is a simple table of airport codes and corresponding cities.
https://datahub.io/core/airport-codes#data 

## Project Deployment
The deployment and infrastructure of this project uses AWS cloudformation. I refer this [post](https://aws.amazon.com/tw/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/) to apply Amazon EMR and Apache Livy to construct my project. below picture is this project data pipline structure.

![data pipeline](https://i.imgur.com/wvRCqjI.png)

## Explore and Assess the Data
In this project, I want to focus on the relationship between U.S.A immigrant and their destination city.

## Define the Data Model
![data model](https://i.imgur.com/cUSH6Og.jpg)


## Construct ETL Pipeline
## Scenarios
