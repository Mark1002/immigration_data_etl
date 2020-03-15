"""Transform all i94mapping text data to parquet."""
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


def preprocess_airport() -> list:
    """Preprocess airport table."""
    result = []
    raw_text = spark.read.text('s3a://<s3-bucket>/data/mapping/i94prtl.txt')
    raw_list = [str(row.value) for row in raw_text.collect()]
    for line in raw_list:
        line = line.replace("'", "").strip()
        airport_code, name = tuple(
            map(lambda x: x.strip(), line.split('='))
        )
        temp = list(map(lambda x: x.strip(), name.split(',')))
        if len(temp) > 1:
            state_code = temp[-1]
            airport_name = ",".join(temp[:-1])
        else:
            airport_name = temp[0]
            state_code = None
        result.append({
            'airport_code': airport_code,
            'airport_name': airport_name,
            'state_code': state_code
        })
    return result


def transform_airport() -> DataFrame:
    """Transform to airport dataframe."""
    airport_list = preprocess_airport()
    airport_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            airport_list
        )
    )
    return airport_df


def preprocess_country() -> list:
    """Preprocess country table."""
    result = []
    raw_text = spark.read.text('s3a://<s3-bucket>/data/mapping/i94cntyl.txt')
    raw_list = [str(row.value) for row in raw_text.collect()]
    for line in raw_list:
        line = line.replace("'", "").strip()
        country_code, country_name = tuple(
            map(lambda x: x.strip(), line.split('='))
        )
        result.append(
            {'country_code': country_code, 'country_name': country_name}
        )
    return result


def transform_country() -> DataFrame:
    """Transform to country_type dataframe."""
    country_list = preprocess_country()
    country_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            country_list
        )
    )
    return country_df


def preprocess_state() -> list:
    """Preprocess state table."""
    result = []
    raw_text = spark.read.text('s3a://<s3-bucket>/data/mapping/i94addrl.txt')
    raw_list = [str(row.value) for row in raw_text.collect()]
    for line in raw_list:
        line = line.replace("'", "").strip()
        state_code, state_name = tuple(line.split('='))
        result.append({'state_code': state_code, 'state_name': state_name})
    return result


def transform_state() -> DataFrame:
    """Transform to state dataframe."""
    state_list = preprocess_state()
    state_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            state_list
        )
    )
    return state_df


def preprocess_transport_type() -> list:
    """Preprocess transport_type table."""
    result = []
    raw_text = spark.read.text('s3a://<s3-bucket>/data/mapping/i94model.txt')
    raw_list = [str(row.value) for row in raw_text.collect()]
    for line in raw_list:
        line = line.replace("'", "").strip()
        transport_code, transport_name = tuple(
            map(lambda x: x.strip(), line.split('='))
        )
        result.append(
            {
                'transport_code': transport_code,
                'transport_name': transport_name
            }
        )
    return result


def transform_transport_type() -> DataFrame:
    """Transform to transport_type dataframe."""
    transport_list = preprocess_transport_type()
    transport_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            transport_list
        )
    )
    return transport_df


def preprocess_visa_type() -> list:
    """Preprocess visa_type table."""
    result = []
    raw_text = spark.read.text('s3a://<s3-bucket>/data/mapping/i94visa.txt')
    raw_list = [str(row.value) for row in raw_text.collect()]
    for line in raw_list:
        line = line.replace("'", "").strip()
        visa_code, visa_name = tuple(
            map(lambda x: x.strip(), line.split('='))
        )
        result.append(
            {'visa_code': visa_code, 'visa_name': visa_name}
        )
    return result


def transform_visa_type() -> DataFrame:
    """Transform to visa_type dataframe."""
    visa_list = preprocess_visa_type()
    visa_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            visa_list
        )
    )
    return visa_df


def main():
    """Main entry point."""
    airport_df = transform_airport()
    airport_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/airport.parquet'
    )
    country_df = transform_country()
    country_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/country.parquet'
    )
    state_df = transform_state()
    state_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/state.parquet'
    )
    transport_df = transform_transport_type()
    transport_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/transport_type.parquet'
    )
    visa_df = transform_visa_type()
    visa_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/visa_type.parquet'
    )


main()
