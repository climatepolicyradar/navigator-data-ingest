import sys

import boto3

from integration_tests.s3_utils import (
    build_bucket,
    upload_file_to_bucket,
    read_local_s3_json_file,
)


def setup_test_data(
    document_bucket_name: str,
    pipeline_bucket_name: str,
    region: str,
    test_data_file_path: str,
    test_data_upload_path: str,
    ingest_output_prefix: str,
) -> None:
    """Setup integration_tests data and infrastructure for the integration integration_tests."""
    s3_conn = boto3.client("s3", region_name=region)
    location = {"LocationConstraint": region}

    build_bucket(s3=s3_conn, bucket_name=document_bucket_name, location=location)
    build_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name, location=location)

    # Upload the integration_tests input json file with updates and new documents
    upload_file_to_bucket(
        s3=s3_conn,
        local_file_path=test_data_file_path,
        bucket_name=pipeline_bucket_name,
        upload_path=test_data_upload_path,
    )

    # Upload the integration_tests document that will be archived
    existing_document_name = list(
        read_local_s3_json_file(test_data_file_path)["updated_documents"].keys()
    )[0]

    upload_file_to_bucket(
        s3=s3_conn,
        local_file_path=test_data_file_path,
        bucket_name=pipeline_bucket_name,
        upload_path=f"{ingest_output_prefix}/{existing_document_name}.json",
    )


if __name__ == "__main__":
    setup_test_data(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
        test_data_file_path=sys.argv[4],
        test_data_upload_path=sys.argv[5],
        ingest_output_prefix=sys.argv[6],
    )
