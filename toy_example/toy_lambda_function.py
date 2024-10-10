import base64
import datetime
import glob
import json
import os
from toy_example.mock_cloudwatch import MockCloudWatch


def make_staging_directories(base_path):
    output_path = os.path.join(
        base_path,
        f"year={datetime.datetime.now().strftime('%Y')}/"
        f"month={datetime.datetime.now().strftime('%m')}/"
        f"day={datetime.datetime.now().strftime('%d')}/hour={datetime.datetime.now().strftime('%H')}/"  # We will partition by minute here as a proof of concept for the toy example
        # For a normal staging area, this would usually be overkill
        f"minute={datetime.datetime.now().strftime('%M')}",
    )
    os.makedirs(output_path, exist_ok=True)
    return output_path


def write_ndjson(output_path, filename, records):
    file_mode = "w" if not os.path.exists(f"{output_path}/{filename}") else "a"
    with open(f"{output_path}/{filename}", file_mode) as f:
        for record in records:
            json_record = json.dumps(record)  # Convert dict to JSON string
            f.write(
                json_record + "\n"
            )  # Append newline character to ensure NDJSON format


def toy_lambda_function(
    cloudwatch_instance: MockCloudWatch, kinesis_dict, redis_instance
) -> None:
    # LAMBDA Function: Extract data from Kinesis and Transform

    # TODO Make function handle empty events / check expected number of event variables
    # TODO add unit tests

    ingested_records = []

    cloudwatch_instance.lambda_invocations += 1

    for record in kinesis_dict["Records"]:
        # Decode the base64-encoded data
        data = json.loads(base64.b64decode(json.loads(record)["kinesis"]["data"]))
        # De-Duplication:
        if not redis_instance.sismember("processed_uuids", data["event_uuid"]):
            redis_instance.sadd("processed_uuids", data["event_uuid"])
            ingested_records.append(data)
        else:
            cloudwatch_instance.duplicates_prevented += 1

    cloudwatch_instance.ingested_events += len(ingested_records)

    # Extract additional fields from the ingested records
    for record in ingested_records:
        record["event_type"] = record["event_name"].split(":")[0]
        record["event_subtype"] = record["event_name"].split(":")[1]
        record["created_datetime"] = datetime.datetime.fromtimestamp(
            record["created_at"]
        ).isoformat()

    output_path = make_staging_directories(base_path="output/datalake/staging/")

    if len(ingested_records) > 0:
        write_ndjson(output_path, "ingested_records.ndjson", ingested_records)
    else:
        print("No records to be written!")

    staging_data_size_in_mb = round(
        (
            sum(
                os.path.getsize(f)
                for f in glob.glob("output/datalake/staging/" + "**/*", recursive=True)
            )
            / (1024 * 1024)
        ),
        2,
    )
    cloudwatch_instance.staging_storage_used_in_mbytes += staging_data_size_in_mb
