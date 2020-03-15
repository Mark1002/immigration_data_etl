"""Transform to visa_type parquet."""
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


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
    visa_df = transform_visa_type()
    visa_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/visa_type.parquet'
    )


main()
