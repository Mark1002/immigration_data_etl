"""Transform to immigration parquet."""
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
    IntegerType, DateType
)


def transform_immigration() -> DataFrame:
    """Transform to immigration dataframe."""
    # load raw data
    df_spark = spark.read.format(
        'com.github.saurfang.sas.spark'
    ).load(
        's3a://<s3-bucket>/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    )
    # exclude cic not used and other column
    # cic not used: visapost, occup, entdepa,
    # entdepd, entdepu, dtaddto, insnum
    # too many null: insnum
    # useless: count
    # relate field: i94bir
    exclude = {
        'visapost', 'occup', 'entdepa',
        'entdepd', 'entdepu', 'dtaddto',
        'count', 'dtadfile', 'insnum', 'i94bir'
    }
    col_list = list(set(df_spark.columns) - exclude)
    # rename column
    # i94addr -> state_code
    # i94cit -> birth_country
    # i94res -> residence_country
    # i94port -> airport_code
    # i94visa -> visa_code
    # i94mode -> transport_code
    # biryear -> birth_year
    # i94year -> year
    # i94mon -> month
    imm_df = df_spark.select(col_list)
    imm_df = imm_df.withColumnRenamed('arrdate', 'arrival_date')\
        .withColumnRenamed('biryear', 'birth_year')\
        .withColumnRenamed('depdate', 'departure_date')\
        .withColumnRenamed('i94cit', 'birth_country')\
        .withColumnRenamed('i94res', 'residence_country')\
        .withColumnRenamed('i94addr', 'state_code')\
        .withColumnRenamed('i94port', 'airport_code')\
        .withColumnRenamed('i94visa', 'visa_code')\
        .withColumnRenamed('i94mode', 'transport_code')\
        .withColumnRenamed('i94yr', 'year')\
        .withColumnRenamed('i94mon', 'month')
    # cast column data type
    imm_df = imm_df.withColumn('birth_country', col('birth_country').cast(
        IntegerType()))\
        .withColumn('month', col('month').cast(IntegerType()))\
        .withColumn('year', col('year').cast(IntegerType()))\
        .withColumn('birth_year', col('birth_year').cast(IntegerType()))\
        .withColumn('visa_code', col('visa_code').cast(IntegerType()))\
        .withColumn('cicid', col('cicid').cast(IntegerType()))\
        .withColumn('residence_country', col('residence_country').cast(
            IntegerType()))\
        .withColumn('transport_code', col('transport_code').cast(
            IntegerType()))\
        .withColumn('admnum', col('admnum').cast(IntegerType()))
    # reorder column
    imm_df = imm_df.select([
        'cicid', 'month', 'year',
        'birth_country', 'residence_country', 'state_code',
        'airport_code', 'visa_code', 'transport_code',
        'arrival_date', 'departure_date', 'gender',
        'birth_year', 'visatype', 'airline',
        'fltno', 'admnum', 'matflag'
    ])
    # udf to preprocess time

    def parse_date(x):
        try:
            result = datetime(1960, 1, 1) + timedelta(days=int(x))
        except Exception:
            result = None
        return result

    parse_date_udf = udf(
        lambda x: parse_date(x),
        DateType()
    )
    # imm df
    imm_df = imm_df.withColumn(
        'arrival_date', parse_date_udf('arrival_date'))\
        .withColumn('departure_date', parse_date_udf('departure_date'))
    # filter only air
    imm_df = imm_df.filter(imm_df.transport_code == 1)
    return imm_df


def main():
    """Main entry point."""
    imm_df = transform_immigration()
    imm_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/immigration/i94_apr16_sub.parquet'
    )


main()
