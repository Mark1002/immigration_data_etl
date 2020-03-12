"""Transform raw data by spark."""
from pyspark.sql import SparkSession

from data_preprocess import (
    preprocess_airport, preprocess_country,
    preprocess_state, preprocess_transport_type,
    preprocess_visa_type
)


class SparkTransformModule:
    """Spark dataframe transform module."""

    def __init__(self):
        """Init."""
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        """Init spark instance."""
        spark = SparkSession.builder.\
            config(
                "spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"
            ).enableHiveSupport().getOrCreate()
        return spark

    def transform_airport_detail(self):
        """Transform to airport_detail dataframe."""
        pass

    def transform_cities_demographics(self):
        """Transform to cities_demographics dataframe."""
        pass

    def transform_immigration(self):
        """Transform to immigration dataframe."""
        pass

    def transform_state(self):
        """Transform to state dataframe."""
        pass

    def transform_airport(self):
        """Transform to airport dataframe."""
        pass

    def transform_country(self):
        """Transform to country dataframe."""
        pass

    def transform_transport_type(self):
        """Transform to transport_type dataframe."""
        pass

    def transform_visa_type(self):
        """Transform to visa_type dataframe."""
        pass

    def transform_imm_city_demographics(self):
        """Transform to imm_city_demographics dataframe."""
        pass
