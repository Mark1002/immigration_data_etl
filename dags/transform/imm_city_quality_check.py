"""Data quality check for imm_city."""
from py4j.protocol import Py4JError


def check():
    imm_city_df = spark.read.parquet(
            's3a://<s3-bucket>/processed/imm_{0}_city_demographics.parquet'.format(month_year) # noqa
        )
    result = imm_city_df.count()
    if result == 0:
        raise Py4JError('data count is 0!')


check()
