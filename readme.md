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
The deployment and infrastructure of this project uses AWS cloudformation. I refer this [post](https://aws.amazon.com/tw/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/) to apply Amazon EMR and Apache Livy to construct my project. below picture is this project data pipeline structure.

![data pipeline](https://i.imgur.com/wvRCqjI.png)

If you want to deploy the project, you should upload the cloudformation yaml file to AWS cloudformation first.Then, AWS cloudformation can create stack according the cloudformation yaml file.

## Explore and Assess the Data
In this project, I want to focus on the relationship between U.S.A immigrant and their landing destination city. below are the data explore steps in each dataset.

* **I94 Immigration Data**
1. I inspect the meta data file, `I94_SAS_Labels_Descriptions.SAS` to know each column's meaning and extract some mapping column to 5 txt files using for immigration data field relation. These 5 txt files are `i94addrl.txt`, `i94cntyl.txt`, `i94model.txt`, `i94prtl.txt`, `i94visa.txt`.
2. In this step, according to the meta data file, I drop all `cic not used` column and too many null value column from the sas file,`i94_apr16_sub.sas7bdat`. Then, I convert the `depdate` and `arrdate` these two columns to datetime format. And I filter out all records that transport type is not air.

* **Airport Code Table**
1. This dataset is really important. Because it contain all airports' detail information. Including the detail location about the airport's city. With this dataset in the middle, I can use it to join immigration data and city demographic data in order to find the relationship with U.S.A immigrant and theirdestination city.
2. In this dataset, I filter all closed airport and not U.S.A airport records.Then, I rename some column to Unify format with others table.likes `municipality -> city`, `iata_code -> airport_code`.

* **City Demographic Data**
1. I drop `Count`and `race` two column because `Count` column isn't having meaning, and `race` column make city demographic data too duplicate.

## Define the Data Model
After exploring and assessing these dataset, I can define the data model.
I define 9 table totally.

**1. airport**
This table is transformed from mapping txt file, `i94prtl.txt`.

**2. state**
This table is transformed from mapping txt file, `i94addrl.txt`.

**3. country**
This table is transformed from mapping txt file, `i94cntyl.txt`.

**4. transport_type**
This table is transformed from mapping txt file, `i94model.txt`.

**5. visa_type**
This table is transformed from mapping txt file, `i94visa.txt`.
Above 5 table can be join with `immigration` table.

**6. immigration**
This table is transformed from sas file.`state_code`, `airport_code` can be used to join with `airport_detail` table.

**7. airport_detail**
This table is transformed from `airport-codes_csv.csv`. `state_code`, `airport_code` can be used to join with `immigration` table, and `city` column can be further use to join with `cities_demographic` table.

**8. cities_demographic**
This table is transformed from `us-cities-demographics.csv`.

**9. imm_city_demographic**
This table is the fact table, the joined result with `immigration`, `airport_detail` and `cities_demographic`.

![data model](https://i.imgur.com/cUSH6Og.jpg)


## Construct ETL Pipeline
Below picture is this project airflow data pipeline, According to the data preprocess step to create the mapping task. And also add data quility check.
This pipeline is executed monthly from Jan 2016 to Dec 2016.

![](https://i.imgur.com/QJ908wU.png)

## Scenarios
**The data was increased by 100x**

We can increase the node number of the EMR cluster to increase the compute power of parallel processing.

**The pipelines would be run on a daily basis by 7 am every day**

We can easily adjust the airflow dag's parameter to achieve this setting.

**The database needed to be accessed by 100+ people**

We can increase the node number of the EMR cluster to increase the compute power of parallel.
