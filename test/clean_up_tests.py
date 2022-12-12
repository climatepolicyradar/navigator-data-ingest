import sys

import boto3


def teardown_test_data(document_bucket_name: str, pipeline_bucket_name: str) -> None:
    """
    Teardown test data for the integration tests.
    """
    s3 = boto3.resource('s3')


    s3.remove_bucket(document_bucket_name)
    s3.remove_bucket(pipeline_bucket_name)


if __name__ == "__main__":
    teardown_test_data(document_bucket_name=sys.argv[1], pipeline_bucket_name=sys.argv[2])
