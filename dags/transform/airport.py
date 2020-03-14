"""Transform to airport parquet."""
import s3fs
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


def preprocess_airport() -> list:
    """Preprocess airport table."""
    fs = s3fs.S3FileSystem(anon=True)
    result = []
    with fs.open('<s3-bucket>/mapping/i94prtl.txt', 'r') as file:
        for line in file:
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


def transform_airport(airport_list: list) -> DataFrame:
    """Transform to airport dataframe."""
    airport_df = spark.createDataFrame(
        map(
            lambda d: Row(**OrderedDict(sorted(d.items()))),
            airport_list
        )
    )
    return airport_df


def main():
    """Main entry point."""
    airport_list = preprocess_airport()
    airport_df = transform_airport(airport_list)
    airport_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/airport.parquet'
    )


main()
