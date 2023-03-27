import glob
import sys
from pathlib import Path

import boto3

from integration_tests.s3_utils import (
    build_bucket,
    upload_file_to_bucket,
)


def setup_test_data(
    document_bucket_name: str,
    pipeline_bucket_name: str,
    region: str,
    test_data_file_path: str,
    test_data_upload_path: str,
    ingest_output_prefix: str,
    embeddings_input_prefix: str,
    indexer_input_prefix: str,
) -> None:
    """Setup integration_tests data and infrastructure for the integration integration_tests."""
    s3_conn = boto3.client("s3", region_name=region)
    location = {"LocationConstraint": region}

    build_bucket(s3=s3_conn, bucket_name=document_bucket_name, location=location)
    build_bucket(s3=s3_conn, bucket_name=pipeline_bucket_name, location=location)

    # Upload the input json file with updates and new documents that will be read by the ingest stage
    upload_file_to_bucket(
        s3=s3_conn,
        local_file_path=test_data_file_path,
        bucket_name=pipeline_bucket_name,
        upload_path=test_data_upload_path,
    )

    # Upload the documents to the s3 pipeline cache directories that will be updated by the integration_tests
    parser_input_files = [
        Path(file)
        for file in glob.glob("integration_tests/data/pipeline/parser_input/*.json")
    ]
    for file in parser_input_files:
        upload_file_to_bucket(
            s3=s3_conn,
            local_file_path=str(file),
            bucket_name=pipeline_bucket_name,
            upload_path=f"{ingest_output_prefix}/{file.name}",
        )

    embeddings_input_files = [
        Path(file)
        for file in glob.glob("integration_tests/data/pipeline/embeddings_input/*.json")
    ]
    for file in embeddings_input_files:
        upload_file_to_bucket(
            s3=s3_conn,
            local_file_path=str(file),
            bucket_name=pipeline_bucket_name,
            upload_path=f"{embeddings_input_prefix}/{file.name}",
        )

    indexer_input_files = [
        Path(file)
        for file in glob.glob("integration_tests/data/pipeline/indexer_input/*.json")
    ]
    for file in indexer_input_files:
        upload_file_to_bucket(
            s3=s3_conn,
            local_file_path=str(file),
            bucket_name=pipeline_bucket_name,
            upload_path=f"{indexer_input_prefix}/{file.name}",
        )

        upload_file_to_bucket(
            s3=s3_conn,
            local_file_path="integration_tests/data/pipeline/indexer_input/indexer_input.npy",
            bucket_name=pipeline_bucket_name,
            upload_path=f"{indexer_input_prefix}/{file.stem + '.npy'}",
        )


if __name__ == "__main__":
    setup_test_data(
        document_bucket_name=sys.argv[1],
        pipeline_bucket_name=sys.argv[2],
        region=sys.argv[3],
        test_data_file_path=sys.argv[4],
        test_data_upload_path=sys.argv[5],
        ingest_output_prefix=sys.argv[6],
        indexer_input_prefix=sys.argv[7],
        embeddings_input_prefix=sys.argv[8],
    )
