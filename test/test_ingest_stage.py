from cloudpathlib import S3Path
import os

from test.s3_utils import create_dictionary_from_s3_bucket, read_local_s3_json_file


INGEST_PIPELINE_BUCKET = os.environ.get("INGEST_PIPELINE_BUCKET")
INGEST_OUTPUT_PREFIX = os.environ.get("INGEST_OUTPUT_PREFIX")
DOCUMENT_NAME_KEY = os.environ.get("DOCUMENT_NAME_KEY")
TEST_DATA_EXPECTED_OUTPUT_FILE_PATH = os.environ.get("TEST_DATA_EXPECTED_OUTPUT_FILE_PATH")

output_data = create_dictionary_from_s3_bucket(
    bucket_path=S3Path(f"s3://{INGEST_PIPELINE_BUCKET}/{INGEST_OUTPUT_PREFIX}/"),
    name_key=DOCUMENT_NAME_KEY)
expected_output_data = read_local_s3_json_file(file_path=TEST_DATA_EXPECTED_OUTPUT_FILE_PATH)


def test_parser_input():
    """
    Assert that the output data from the ingest stage is as expected.
    """
    assert output_data == expected_output_data
