"""Transform to country parquet."""
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


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


def main():
    """Main entry point."""
    country_df = transform_country()
    country_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/country.parquet'
    )


main()
