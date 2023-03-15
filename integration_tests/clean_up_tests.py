import sys

import boto3

from integration_tests.s3_utils import remove_bucket, remove_objects


def tear_down_test_data(
    document_bucket_name: str, pipeline_bucket_name: str, region: str
) -> None:
    """Remove the AWS infrastructure used in the unit integration_tests."""
    s3_conn = boto3.resource("s3", region_name=region)

    remove_objects(s3=s3_conn, bucket_name=document_bucket_name)
    remove_objects(s3=s3_conn, bucket_name=pipeline_bucket_name)

    s3_conn = boto3.client("s3", region_name=region)

    remove_bucket(s3=s3_conn, bucket_name=document_bucket_name)
    remove_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name)


if __name__ == "__main__":
    tear_down_test_data(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
    )
