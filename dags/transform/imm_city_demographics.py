"""Transform to imm_city_demographics parquet."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_imm_city_demographics(
        imm_df: DataFrame, airport_detail_df: DataFrame,
        demo_graph_df: DataFrame
) -> DataFrame:
    """Transform to imm_city_demographics dataframe."""
    t1 = imm_df.alias('t1')
    t2 = airport_detail_df.alias('t2')
    # imm_df join with airport_detail_df to get city column
    imm_city_df = t1.join(t2, [
        t1.state_code == t2.state_code,
        t1.airport_code == t2.airport_code
    ]).select(
        col('t1.cicid').alias('cicid'),
        col('t1.month').alias('month'),
        col('t1.year').alias('year'),
        col('t1.birth_country').alias('birth_country'),
        col('t1.arrival_date').alias('arrival_date'),
        col('t1.departure_date').alias('departure_date'),
        col('t1.birth_year').alias('birth_year'),
        col('t1.gender').alias('gender'),
        col('t2.city').alias('city'),
        col('t2.name').alias('airport_name'),
        col('t1.state_code').alias('state_code'),
        col('t1.airport_code').alias('airport_code'),
        col('t1.visa_code').alias('visa_code')
    )
    # imm_city_df join with demo_graph_df to get city demographics info
    t1 = imm_city_df.alias('t1')
    t2 = demo_graph_df.alias('t2')
    imm_city_demograph_df = t1.join(t2, t1.city == t2.city).select(
        col('t1.cicid').alias('cicid'),
        col('t1.month').alias('month'),
        col('t1.year').alias('year'),
        col('t1.birth_country').alias('birth_country'),
        col('t1.arrival_date').alias('arrival_date'),
        col('t1.departure_date').alias('departure_date'),
        col('t1.birth_year').alias('birth_year'),
        col('t1.gender').alias('gender'),
        col('t1.city').alias('city'),
        col('t2.median_age').alias('city_median_age'),
        col('t2.male_population').alias('city_male_population'),
        col('t2.female_population').alias('city_female_population'),
        col('t2.total_population').alias('city_total_population'),
        col('t2.veteran_count').alias('city_veteran_count'),
        col('t2.foreign_born').alias('city_foreign_born_count'),
        col('t1.airport_name').alias('airport_name'),
        col('t1.state_code').alias('state_code'),
        col('t1.airport_code').alias('airport_code'),
        col('t1.visa_code').alias('visa_code')
    )
    return imm_city_demograph_df


def main():
    """Main entry point."""
    imm_df = spark.read.parquet(
        's3a://<s3-bucket>/processed/immigration/i94_{0}_sub.parquet'.format(month_year) # noqa
    )
    airport_detail_df = spark.read.parquet(
        's3a://<s3-bucket>/processed/airport_detail.parquet'
    )
    demo_graph_df = spark.read.parquet(
        's3a://<s3-bucket>/processed/cities_demographics.parquet'
    )
    imm_city_demograph_df = transform_imm_city_demographics(
        imm_df, airport_detail_df, demo_graph_df
    )
    imm_city_demograph_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/imm_{0}_city_demographics.parquet'.format(month_year) # noqa
    )


main()
