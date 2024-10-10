import os
import sys
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from unittest.mock import mock_open, patch, MagicMock, call
import pytest
from toy_example.toy_lambda_function import write_ndjson

@pytest.mark.parametrize("existing_file", [False, True])
def test_write_ndjson(existing_file):
    output_path = "some/path"
    filename = "test.ndjson"
    records = [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]

    # Prepare the path/filename
    full_path = f'{output_path}/{filename}'
    
    # Mock os.path.exists to control file existence simulation
    with patch('os.path.exists', return_value=existing_file):
        # Mock open to simulate file operations
        m = mock_open()
        with patch('builtins.open', m) as mocked_file:
            write_ndjson(output_path, filename, records)

            # Ensure open is called correctly, with 'w' for new file or 'a' for existing file
            file_mode = 'w' if not existing_file else 'a'
            mocked_file.assert_called_once_with(full_path, file_mode)

            # Handle calls to the file handle
            handle = mocked_file()
            expected_calls = [call(json.dumps(rec) + '\n') for rec in records]
            handle.write.assert_has_calls(expected_calls, any_order=True)

def test_ndjson_format():
    records = [{"id": 1, "value": "A"}]
    expected_output = json.dumps(records[0]) + '\n'

    with patch('builtins.open', mock_open()) as mocked_file:
        write_ndjson("some/path", "test.ndjson", records)

        mocked_file().write.assert_called_once_with(expected_output)

