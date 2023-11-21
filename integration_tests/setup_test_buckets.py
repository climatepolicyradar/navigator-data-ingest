import sys

import boto3

from integration_tests.s3_utils import build_bucket


def build(
    document_bucket_name: str,
    pipeline_bucket_name: str,
    region: str,
) -> None:
    """Setup integration_tests data and infrastructure for the integration tests."""
    s3_conn = boto3.client("s3", region_name=region)
    location = {"LocationConstraint": region}

    build_bucket(s3=s3_conn, bucket_name=document_bucket_name, location=location)
    build_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name, location=location)


if __name__ == "__main__":
    build(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
    )
