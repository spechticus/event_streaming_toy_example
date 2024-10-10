import json
import base64
import sys
import os
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from toy_example.toy_lambda_function import toy_lambda_function
from toy_example.mock_cloudwatch import MockCloudWatch


@pytest.fixture
def cloudwatch_instance():
    return MockCloudWatch()


@pytest.fixture
def redis_instance():
    redis_mock = MagicMock()
    redis_mock.sismember = MagicMock(return_value=False)
    redis_mock.sadd = MagicMock()
    return redis_mock


@pytest.fixture
def kinesis_event():
    return {
        "Records": [
            json.dumps(
                {
                    "kinesis": {
                        "data": base64.b64encode(
                            json.dumps(
                                {
                                    "event_uuid": "uuid1",
                                    "event_name": "type:subtype",
                                    "created_at": 1609459200,
                                }
                            ).encode()
                        ).decode()
                    }
                }
            )
        ]
    }


def test_toy_lambda_function_no_duplicates(
    cloudwatch_instance, redis_instance, kinesis_event
):
    with patch(
        "toy_example.toy_lambda_function.os.path.getsize", return_value=100
    ), patch(
        "toy_example.toy_lambda_function.glob.glob", return_value=["file1", "file2"]
    ), patch(
        "toy_example.toy_lambda_function.write_ndjson"
    ) as mock_write_ndjson, patch(
        "toy_example.toy_lambda_function.make_staging_directories",
        return_value="output/datalake/staging/",
    ):

        toy_lambda_function(cloudwatch_instance, kinesis_event, redis_instance)

        # Check if CloudWatch metrics are updated correctly
        assert cloudwatch_instance.lambda_invocations == 1
        assert cloudwatch_instance.duplicates_prevented == 0
        assert cloudwatch_instance.ingested_events == 1

        # Check Redis interaction
        redis_instance.sismember.assert_called_once_with("processed_uuids", "uuid1")
        redis_instance.sadd.assert_called_once_with("processed_uuids", "uuid1")

        # Ensure NDJSON file is written
        mock_write_ndjson.assert_called_once()


def test_toy_lambda_function_with_duplicates(
    cloudwatch_instance, redis_instance, kinesis_event
):
    redis_instance.sismember = MagicMock(return_value=True)

    with patch(
        "toy_example.toy_lambda_function.os.path.getsize", return_value=100
    ), patch(
        "toy_example.toy_lambda_function.glob.glob", return_value=["file1", "file2"]
    ), patch(
        "toy_example.toy_lambda_function.write_ndjson"
    ) as mock_write_ndjson, patch(
        "toy_example.toy_lambda_function.make_staging_directories",
        return_value="output/datalake/staging/",
    ):

        toy_lambda_function(cloudwatch_instance, kinesis_event, redis_instance)

        # Check for duplicates handling
        assert cloudwatch_instance.duplicates_prevented == 1
        assert cloudwatch_instance.ingested_events == 0
        assert cloudwatch_instance.lambda_invocations == 1

        # Redis should prevent adding duplicate
        redis_instance.sismember.assert_called_once_with("processed_uuids", "uuid1")
        redis_instance.sadd.assert_not_called()

        # No data should be written to NDJSON since it's a duplicate
        mock_write_ndjson.assert_not_called()


def test_toy_lambda_function_no_records_to_write(
    cloudwatch_instance, redis_instance, kinesis_event
):
    # Configure Redis to simulate all events as duplicates
    redis_instance.sismember.return_value = True

    with patch("toy_example.toy_lambda_function.write_ndjson") as mock_write_ndjson:
        toy_lambda_function(cloudwatch_instance, kinesis_event, redis_instance)

        # Ensure write_ndjson is not called when there are no records to process
        mock_write_ndjson.assert_not_called()

        # Check CloudWatch metrics for duplicates and invocations
        assert cloudwatch_instance.lambda_invocations == 1
        assert cloudwatch_instance.duplicates_prevented == len(kinesis_event["Records"])
        assert cloudwatch_instance.ingested_events == 0
