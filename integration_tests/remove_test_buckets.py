import sys

import boto3
from botocore.client import ClientError

from integration_tests.s3_utils import remove_bucket, remove_objects


def remove(document_bucket_name: str, pipeline_bucket_name: str, region: str) -> None:
    """Remove the AWS infrastructure used in the unit integration_tests."""
    s3_resource = boto3.resource("s3", region_name=region)
    s3_client = boto3.client("s3", region_name=region)

    try:
        remove_objects(s3=s3_resource, bucket_name=document_bucket_name)
        remove_bucket(s3=s3_client, bucket_name=document_bucket_name)
    except ClientError:
        pass  # Bucket does not exist

    try:
        remove_objects(s3=s3_resource, bucket_name=pipeline_bucket_name)
        remove_bucket(s3=s3_client, bucket_name=pipeline_bucket_name)
    except ClientError:
        pass  # Bucket does not exist


if __name__ == "__main__":
    remove(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
    )
