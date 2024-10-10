# TODO: write unit tests!

import sys
import os
import redis
from time import sleep
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from toy_example.mock_cloudwatch import MockCloudWatch
from data_creation.producer import toy_data_generation
from toy_example.toy_lambda_function import toy_lambda_function
from toy_example.toy_glue import toy_glue_function

# Set up all relevant instances here to avoid overhead and redundant calls.
cloudwatch = MockCloudWatch()
spark = SparkSession.builder.appName("Event Batch Processing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
r = redis.Redis(host="localhost", port=6379, db=0)

while True:
    # Goal: 1 Million events per hour = 16,666 per minute = 278 per second
    n_records_per_second = 278
    # Every 60 seconds or 33,360 events, batch processing will happen
    batch_processing_delay = 60
    for second in range(batch_processing_delay):
        for _ in range(n_records_per_second):
            toy_lambda_function(
                kinesis_dict=toy_data_generation(n=1),
                cloudwatch_instance=cloudwatch,
                redis_instance=r,
            )
        print(
            f"Processed {(second + 1) * n_records_per_second} events ({second + 1} / {batch_processing_delay})"
        )
        sleep(1)

    toy_glue_function(spark_session=spark, cloudwatch_instance=cloudwatch)
    print(f"Finished glue invocation {cloudwatch.glue_invocations}")

    cloudwatch.calculate_error_ratio()
    cloudwatch.calculate_used_storage_percentage()

    cloudwatch.to_markdown("output/cloudwatch_reports/")
    print("Successfully compiled cloudwatch report!")
    print(
        """Re-starting another cycle.
You need to manually terminate this script with CTRL-C."""
    )
