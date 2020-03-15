"""Transform to cities_demographics parquet."""
from pyspark.sql import DataFrame


def transform_cities_demographics() -> DataFrame:
    """Transform to cities_demographics dataframe."""
    demo_graph_df = spark.read.csv(
        's3a://<s3-bucket>/data/us-cities-demographics.csv', sep=';',
        header=True, inferSchema=True
    )
    demo_graph_df = demo_graph_df.drop('Count', 'Race')
    demo_graph_df = demo_graph_df.dropDuplicates()
    demo_graph_df = demo_graph_df.withColumnRenamed('City', 'city')\
        .withColumnRenamed('State', 'state')\
        .withColumnRenamed('Median Age', 'median_age')\
        .withColumnRenamed('Male Population', 'male_population')\
        .withColumnRenamed('Female Population', 'female_population')\
        .withColumnRenamed('Total Population', 'total_population')\
        .withColumnRenamed('Number of Veterans', 'veteran_count')\
        .withColumnRenamed('Foreign-born', 'foreign_born')\
        .withColumnRenamed(
            'Average Household Size', 'average_household_size')\
        .withColumnRenamed('State Code', 'state_code')
    return demo_graph_df


def main():
    """Main entry point."""
    demo_graph_df = transform_cities_demographics()
    demo_graph_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/cities_demographics.parquet'
    )


main()
