import datetime
import glob
import os

from pyspark.sql.functions import col, explode
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from toy_example.mock_cloudwatch import MockCloudWatch


def toy_glue_function(
    spark_session,
    cloudwatch_instance: MockCloudWatch,
    input_hour=datetime.datetime.now().hour,
) -> None:
    # GLUE or EMR function: Batch load to permanent layer, batch deduplicate,

    cloudwatch_instance.glue_invocations += 1

    input_paths = glob.glob(
        f"output/datalake/staging/year={datetime.datetime.now().strftime('%Y')}/"
        f"month={datetime.datetime.now().strftime('%m')}/"
        f"day={datetime.datetime.now().strftime('%d')}/hour={input_hour}/*/*.ndjson"
    )

    # Read in the sample json which should be big enough to correctly instruct Spark to infer the schema
    # In the end, it is simpler and more elegant to just create enough sample events than to manually specify the schema of all possible event_specifics
    sample_json = spark_session.read.json("toy_example/inference_events.json")

    df = spark_session.read.json(input_paths, schema=sample_json.schema)

    # Correct? Needs to match the number of records ingested // number of lines in all files in the directory

    # pull out language_id because we need it for batching later
    transformed_df = df.withColumn(
        "language_id", col("event_specifics.language_id").cast("string")
    ).drop("event_specifics.language_id")

    batch_duplicates = (
        transformed_df.groupBy("event_uuid").count().where("count > 1").count()
    )
    cloudwatch_instance.batch_duplicates += batch_duplicates

    if batch_duplicates > 0:
        transformed_df = transformed_df.dropDuplicates("event_uuid")

    spark_session.conf.set("spark.sql.files.maxRecordsPerBatch", "10000")

    # Using repartition might help better in this case to ensure even data distribution
    processed_folder = f"output/datalake/processed/year={datetime.datetime.now().strftime('%Y')} \
            /month={datetime.datetime.now().strftime('%m')}/day={datetime.datetime.now().strftime('%d')} \
            /hour={input_hour}/"

    repartitioned_df = transformed_df.repartition(2, "language_id")
    repartitioned_df.write.partitionBy("language_id").format("parquet").mode(
        "overwrite"
    ).save(processed_folder)

    processed_data_size_in_mb = round(
        sum(
            os.path.getsize(f)
            for f in glob.glob("output/datalake/processed/" + "**/*", recursive=True)
        )
        / (1024 * 1024),
        2,
    )
    cloudwatch_instance.processed_storage_used_in_mbytes += processed_data_size_in_mb
