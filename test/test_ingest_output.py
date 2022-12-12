from cloudpathlib import S3Path

from test.s3_utils import create_dictionary_from_s3_bucket, read_local_s3_json_file

# .env file
output_data = create_dictionary_from_s3_bucket(
    bucket_path=S3Path("s3://ingest-unit-test-pipeline-bucket/ingest_unit_test_parser_input/"),
    name_key="document_id")
expected_output_data = read_local_s3_json_file(file_path="test/data/docs_test_subset_parser_input_expected.json")


def test_parser_input():
    """
    Assert that the output data from the ingest stage is as expected.
    """
    assert output_data == expected_output_data
