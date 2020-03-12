"""Transform raw data by spark."""
from pyspark.sql import SparkSession


def create_spark_session():
    """Init spark instance."""
    spark = SparkSession.builder.\
        config(
            "spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"
        ).enableHiveSupport().getOrCreate()
    return spark
