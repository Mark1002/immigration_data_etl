"""Data quality check for immigration."""
from py4j.protocol import Py4JError


def check():
    imm_df = spark.read.parquet(
            's3a://<s3-bucket>/processed/immigration/i94_{0}_sub.parquet'.format(month_year) # noqa
        )
    result = imm_df.count()
    if result == 0:
        raise Py4JError('data count is 0!')


check()
