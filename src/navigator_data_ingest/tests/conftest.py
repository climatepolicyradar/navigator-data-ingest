import re
import os
import boto3
import botocore.client
import pytest
from moto import mock_s3

from navigator_data_ingest.base.types import UpdateConfig


class S3Client:
    """Helper class to connect to S3 and perform actions on buckets and documents."""

    def __init__(self, region):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config=botocore.client.Config(
                signature_version="s3v4",
                region_name=region,
                connect_timeout=10,
            ),
        )


@pytest.fixture
def s3_document_id() -> str:
    return "CCLW.executive.1000.1000"


@pytest.fixture
def test_update_config(s3_bucket_and_region) -> UpdateConfig:
    return UpdateConfig(
        pipeline_bucket=s3_bucket_and_region["bucket"],
        input_prefix="input",
        parser_input="parser_input",
        embeddings_input="embeddings_input",
        indexer_input="indexer_input",
        archive_prefix="archive",
        archive_trigger_parser="reparse_trigger",
    )


@pytest.fixture
def s3_document_keys(s3_document_id, test_update_config) -> list:
    return [
        f"{test_update_config.parser_input}/{s3_document_id}.json",
        f"{test_update_config.embeddings_input}/{s3_document_id}.json",
        f"{test_update_config.indexer_input}/{s3_document_id}.json",
        f"{test_update_config.indexer_input}/{s3_document_id}.npy",
    ]


@pytest.fixture
def s3_bucket_and_region() -> dict:
    return {
        "bucket": "cpr-data-pipeline-cache-ingest-integration_tests",
        "region": "eu-west-1",
    }


@pytest.fixture
def test_s3_client(s3_bucket_and_region, s3_document_keys) -> S3Client:
    with mock_s3():
        s3_client = S3Client(s3_bucket_and_region["region"])

        s3_client.client.create_bucket(
            Bucket=s3_bucket_and_region["bucket"],
            CreateBucketConfiguration={
                "LocationConstraint": s3_bucket_and_region["region"]
            },
        )

        for key in s3_document_keys:
            s3_client.client.put_object(
                Bucket=s3_bucket_and_region["bucket"],
                Key=key,
                Body=bytes(1024),
            )

        yield s3_client


@pytest.fixture
def archive_file_pattern() -> dict:
    return {
        "json": re.compile(r"^[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+.json"),
        "npy": re.compile(r"^[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+.npy"),
    }
