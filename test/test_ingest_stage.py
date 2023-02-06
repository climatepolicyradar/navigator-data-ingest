from cloudpathlib import S3Path
import os

from test.s3_utils import create_dictionary_from_s3_bucket, read_local_s3_json_file


INGEST_PIPELINE_BUCKET = os.environ.get("INGEST_PIPELINE_BUCKET")
DOCUMENT_NAME_KEY = os.environ.get("DOCUMENT_NAME_KEY")
INGEST_OUTPUT_PREFIX = os.environ.get("INGEST_OUTPUT_PREFIX")
PARSER_INPUT_EXPECTED_DATA_FILE_PATH = os.environ.get(
    "PARSER_INPUT_EXPECTED_DATA_FILE_PATH"
)
ARCHIVE_EXPECTED_DATA_FILE_PATH = os.environ.get("ARCHIVE_EXPECTED_DATA_FILE_PATH")

parser_input_data = create_dictionary_from_s3_bucket(
    bucket_path=S3Path(f"s3://{INGEST_PIPELINE_BUCKET}/{INGEST_OUTPUT_PREFIX}/"),
    name_key=DOCUMENT_NAME_KEY,
)
expected_parser_input_data = read_local_s3_json_file(
    file_path=PARSER_INPUT_EXPECTED_DATA_FILE_PATH
)

archive_data = create_dictionary_from_s3_bucket(
    bucket_path=S3Path(
        f"s3://{INGEST_PIPELINE_BUCKET}/archive/{INGEST_OUTPUT_PREFIX}/"
    ),
    name_key=DOCUMENT_NAME_KEY,
)
expected_archive_data = read_local_s3_json_file(
    file_path=ARCHIVE_EXPECTED_DATA_FILE_PATH
)


def test_parser_input():
    """Assert that the output data from the ingest stage in s3 is as expected."""
    assert len(parser_input_data.keys()) == len(expected_parser_input_data.keys())
    assert parser_input_data.keys() == expected_parser_input_data.keys()
    assert parser_input_data == expected_parser_input_data


def test_archive():
    """Assert that the output data from the ingest stage in s3 is as expected."""
    assert len(archive_data.keys()) == len(expected_archive_data.keys())
    assert archive_data.keys() == expected_archive_data.keys()
    assert archive_data == expected_archive_data
