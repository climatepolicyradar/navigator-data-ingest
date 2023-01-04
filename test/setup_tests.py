import boto3
import sys

from test.s3_utils import build_bucket, remove_bucket, upload_file_to_bucket, remove_objects


def setup_test_data(document_bucket_name: str, pipeline_bucket_name: str,
                    region: str,  test_data_file_path: str,
                    test_data_upload_path: str) -> None:
    """
    Setup test data and infrastructure for the integration tests.
    """
    s3_conn = boto3.client('s3', region_name=region)
    location = {'LocationConstraint': region}

    build_bucket(s3=s3_conn, bucket_name=document_bucket_name, location=location)
    build_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name, location=location)

    upload_file_to_bucket(s3=s3_conn, local_file_path=test_data_file_path,
                          bucket_name=pipeline_bucket_name, upload_path=test_data_upload_path)


if __name__ == "__main__":
    setup_test_data(document_bucket_name=sys.argv[1], pipeline_bucket_name=sys.argv[2],
                    region=sys.argv[3], test_data_file_path=sys.argv[4],
                    test_data_upload_path=sys.argv[5])
