"""Transform to transport_type parquet."""
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


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


def main():
    """Main entry point."""
    transport_df = transform_transport_type()
    transport_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/transport_type.parquet'
    )


main()
