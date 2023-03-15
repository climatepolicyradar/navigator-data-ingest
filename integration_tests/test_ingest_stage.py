from cloudpathlib import S3Path
import os
import re

from integration_tests.s3_utils import (
    create_dictionary_from_s3_bucket,
    read_local_s3_json_file,
)

ARCHIVE_FILE_PATTERN = re.compile(
    r"^[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+.json"
)
INGEST_PIPELINE_BUCKET = os.environ.get("INGEST_PIPELINE_BUCKET")
DOCUMENT_NAME_KEY = os.environ.get("DOCUMENT_NAME_KEY")
INGEST_OUTPUT_PREFIX = os.environ.get("INGEST_OUTPUT_PREFIX")
PARSER_INPUT_EXPECTED_DATA_FILE_PATH = os.environ.get(
    "PARSER_INPUT_EXPECTED_DATA_FILE_PATH"
)
TEST_DATA_FILE_PATH = os.environ.get("TEST_DATA_FILE_PATH")
EXISTING_DOCUMENT_NAME = list(
    read_local_s3_json_file(TEST_DATA_FILE_PATH)["updated_documents"].keys()
)[0]
parser_input_data = create_dictionary_from_s3_bucket(
    bucket_path=S3Path(f"s3://{INGEST_PIPELINE_BUCKET}/{INGEST_OUTPUT_PREFIX}/"),
    name_key=DOCUMENT_NAME_KEY,
    glob_pattern="*.json",
)
expected_parser_input_data = read_local_s3_json_file(
    file_path=PARSER_INPUT_EXPECTED_DATA_FILE_PATH
)


def test_parser_input():
    """Assert that the output data from the ingest stage in s3 is as expected."""
    assert len(parser_input_data.keys()) == len(expected_parser_input_data.keys())
    assert parser_input_data.keys() == expected_parser_input_data.keys()
    assert parser_input_data == expected_parser_input_data


def test_archive():
    """Assert that the archived data in s3 is as expected."""

    parser_input_archived_files = list(
        S3Path(f"s3://{INGEST_PIPELINE_BUCKET}/archive/{INGEST_OUTPUT_PREFIX}/").glob(
            "*/*.json"
        )
    )
    assert len(parser_input_archived_files) == 1

    archived_file = parser_input_archived_files[0]
    assert (
        str(archived_file.parent)
        == f"s3://{INGEST_PIPELINE_BUCKET}/archive/{INGEST_OUTPUT_PREFIX}/{EXISTING_DOCUMENT_NAME}"
    )
    assert ARCHIVE_FILE_PATTERN.match(archived_file.name)
