import sys

import boto3

from integration_tests.s3_utils import (
    build_bucket,
)


def setup_test_bucket(
    document_bucket_name: str,
    pipeline_bucket_name: str,
    region: str,
) -> None:
    """Setup integration_tests data and infrastructure for the integration integration_tests."""
    # TODO integrate a unique identifier into the bucket name to avoid collisions
    s3_conn = boto3.client("s3", region_name=region)
    location = {"LocationConstraint": region}

    build_bucket(s3=s3_conn, bucket_name=document_bucket_name, location=location)
    build_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name, location=location)


if __name__ == "__main__":
    setup_test_bucket(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
    )
