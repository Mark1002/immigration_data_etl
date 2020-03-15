"""Transform to state parquet."""
from collections import OrderedDict
from pyspark.sql import DataFrame, Row


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


def main():
    """Main entry point."""
    state_df = transform_state()
    state_df.write.mode('overwrite').parquet(
        's3a://<s3-bucket>/processed/state.parquet'
    )


main()
