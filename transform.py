"""Transform raw data by spark."""
from datetime import datetime, timedelta
from collections import OrderedDict
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, udf, split
from pyspark.sql.types import (
    IntegerType, DateType, DoubleType
)

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

    def create_spark_session(self) -> SparkSession:
        """Init spark instance."""
        spark = SparkSession.builder.\
            config(
                "spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"
            ).enableHiveSupport().getOrCreate()
        return spark

    def transform_airport_detail(self) -> DataFrame:
        """Transform to airport_detail dataframe."""
        airport_detail_df = self.spark.read.csv(
            'airport-codes_csv.csv', header=True, inferSchema=True
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

    def transform_cities_demographics(self) -> DataFrame:
        """Transform to cities_demographics dataframe."""
        demo_graph_df = self.spark.read.csv(
            'us-cities-demographics.csv', sep=';',
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

    def transform_immigration(self) -> DataFrame:
        """Transform to immigration dataframe."""
        # load raw data
        df_spark = self.spark.read.format(
            'com.github.saurfang.sas.spark'
        ).load(
            '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
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

    def convert_to_row(self, d: dict) -> Row:
        """Convert dict to spark row."""
        return Row(**OrderedDict(sorted(d.items())))

    def transform_state(self) -> DataFrame:
        """Transform to state dataframe."""
        state_list = preprocess_state()
        state_df = self.spark.createDataFrame(
            map(self.convert_to_row, state_list)
        )
        return state_df

    def transform_airport(self) -> DataFrame:
        """Transform to airport dataframe."""
        airport_list = preprocess_airport()
        airport_df = self.spark.createDataFrame(
            map(self.convert_to_row, airport_list)
        )
        return airport_df

    def transform_country(self) -> DataFrame:
        """Transform to country dataframe."""
        country_list = preprocess_country()
        country_df = self.spark.createDataFrame(
            map(self.convert_to_row, country_list)
        )
        country_df = country_df.withColumn(
            'country_code', col('country_code').cast(IntegerType())
        )
        return country_df

    def transform_transport_type(self) -> DataFrame:
        """Transform to transport_type dataframe."""
        transport_list = preprocess_transport_type()
        transport_df = self.spark.createDataFrame(
            map(self.convert_to_row, transport_list)
        )
        transport_df = transport_df.withColumn(
            'transport_code', col('transport_code').cast(IntegerType())
        )
        return transport_df

    def transform_visa_type(self) -> DataFrame:
        """Transform to visa_type dataframe."""
        visa_list = preprocess_visa_type()
        visa_df = self.spark.createDataFrame(
            map(self.convert_to_row, visa_list)
        )
        visa_df = visa_df.withColumn(
            'visa_code', col('visa_code').cast(IntegerType())
        )
        return visa_df

    def transform_imm_city_demographics(
        self, imm_df: DataFrame, airport_detail_df: DataFrame,
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
