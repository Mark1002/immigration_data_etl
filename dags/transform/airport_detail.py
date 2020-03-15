"""Transform to airport_detail parquet."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split
from pyspark.sql.types import DoubleType


def transform_airport_detail() -> DataFrame:
    """Transform to airport_detail dataframe."""
    airport_detail_df = spark.read.csv(
        's3a://<s3-bucket>/data/airport-codes_csv.csv',
        header=True, inferSchema=True
    )
    airport_detail_df = airport_detail_df.filter(
        (col('iso_country') == 'US') &
        (col('type') != 'closed') &
        (col('iata_code').isNotNull())
    )
    split_func = split(airport_detail_df['coordinates'], ',')
    airport_detail_df = airport_detail_df.withColumn(
        'latitude', split_func.getItem(0).cast(DoubleType())
    ).withColumn('longitude', split_func.getItem(1).cast(DoubleType()))

    split_func = split(airport_detail_df['iso_region'], '-')
    airport_detail_df = airport_detail_df.withColumn(
        'state_code', split_func.getItem(1)
    )
    airport_detail_df = airport_detail_df.drop('coordinates')\
        .drop('continent').drop('gps_code').drop('local_code')\
        .drop('iso_region')

    airport_detail_df = airport_detail_df.withColumnRenamed(
        'ident', 'airport_id'
    ).withColumnRenamed('municipality', 'city')\
        .withColumnRenamed('iata_code', 'airport_code')\
        .withColumnRenamed('iso_country', 'country')
    return airport_detail_df


def main():
    """Main entry point."""
    airport_detail_df = transform_airport_detail()
    airport_detail_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/airport_detail.parquet'
    )


main()
