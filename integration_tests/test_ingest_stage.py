import pytest
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


@pytest.mark.integration
def test_update_1():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'name'

    Document ID:
    - TESTCCLW.executive.1.1

    Expected Result:
    - Document name should be updated in the json objects
    - The npy file should be removed from the indexer input prefix to trigger re-creation
    """

    assert True


@pytest.mark.integration
def test_update_2():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'description'

    Document ID:
    - TESTCCLW.executive.2.2

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_3():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'source_url'

    Document ID:
    - TESTCCLW.executive.3.3

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_4():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'document_status'

    Document ID:
    - TESTCCLW.executive.4.4

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_5():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'document_status' and 'name'

    Document ID:
    - TESTCCLW.executive.5.5

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_6():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'description' and 'source_url'

    Document ID:
    - TESTCCLW.executive.6.6

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_7():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update: - Update to document 'description' (for this document it simulates a document that has is currently being
    processed i.e. doesn't exist in all the s3 prefixes).

    Document ID:
    - TESTCCLW.executive.7.7

    Expected Result:
    """
    assert True


# TODO assert that the documents are uploaded to the document bucket
# TODO assert that the new documents are uploaded to the parser input in the pipeline_initial_state bucket


@pytest.mark.integration
def test_parser_input():
    """Assert that the output data from the ingest stage in s3 is as expected."""
    assert len(parser_input_data.keys()) == len(expected_parser_input_data.keys())
    assert parser_input_data.keys() == expected_parser_input_data.keys()
    assert parser_input_data == expected_parser_input_data
