import json
import os
import re

import boto3
import botocore.client
import pytest
from cpr_sdk.pipeline_general_models import Update, UpdateTypes
from moto import mock_aws

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
def parser_input_json():
    return {
        "document_name": "An example document name.",
        "document_description": "An example document description.",
        "document_id": "CCLW.executive.10000.4494",
        "document_source_url": "https://domain/path/to/document.pdf",
        "document_cdn_object": None,
        "document_content_type": "text/html",
        "document_md5_sum": None,
        "document_metadata": {"publication_ts": "2021-12-25T00:00:00"},
        "document_slug": "an_example_slug_1000_1000",
    }


@pytest.fixture
def embeddings_input_json():
    return {
        "document_name": "An example document name.",
        "document_description": "An example document description.",
        "document_id": "CCLW.executive.10000.4494",
        "document_source_url": "https://domain/path/to/document.pdf",
        "document_cdn_object": None,
        "document_content_type": "text/html",
        "document_md5_sum": None,
        "document_metadata": {"publication_ts": "2021-12-25T00:00:00"},
        "document_slug": "an_example_slug_1000_1000",
        "languages": ["en"],
        "translated": False,
        "html_data": {
            "detected_title": "One Stop Shop Service",
            "detected_date": None,
            "has_valid_text": True,
            "text_blocks": [
                {
                    "text": ["Why use a One Stop Shop"],
                    "text_block_id": "b0",
                    "language": "en",
                    "type": "Text",
                    "type_confidence": 1.0,
                    "coords": None,
                    "page_number": None,
                }
            ],
            "pdf_data": None,
        },
    }


@pytest.fixture
def indexer_input_json():
    return {
        "document_name": "An example document name.",
        "document_description": "An example document description.",
        "document_id": "CCLW.executive.10000.4494",
        "document_source_url": "https://domain/path/to/document.pdf",
        "document_cdn_object": None,
        "document_content_type": "text/html",
        "document_md5_sum": None,
        "document_metadata": {"publication_ts": "2021-12-25T00:00:00"},
        "document_slug": "an_example_slug_1000_1000",
        "languages": ["en"],
        "translated": False,
        "html_data": {
            "detected_title": "One Stop Shop Service",
            "detected_date": None,
            "has_valid_text": True,
            "text_blocks": [
                {
                    "text": ["Why use a One Stop Shop"],
                    "text_block_id": "b0",
                    "language": "en",
                    "type": "Text",
                    "type_confidence": 1.0,
                    "coords": None,
                    "page_number": None,
                }
            ],
            "pdf_data": None,
        },
    }


@pytest.fixture
def test_s3_objects(
    parser_input_json, embeddings_input_json, indexer_input_json, s3_document_keys
):
    return {
        s3_document_keys[0]: bytes(json.dumps(parser_input_json).encode("UTF-8")),
        s3_document_keys[1]: bytes(json.dumps(embeddings_input_json).encode("UTF-8")),
        s3_document_keys[2]: bytes(json.dumps(embeddings_input_json).encode("UTF-8")),
        s3_document_keys[3]: bytes(json.dumps(indexer_input_json).encode("UTF-8")),
        s3_document_keys[4]: bytes(1024),
    }


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
    )


@pytest.fixture
def s3_document_keys(s3_document_id, test_update_config) -> list:
    return [
        f"{test_update_config.parser_input}/{s3_document_id}.json",
        f"{test_update_config.embeddings_input}/{s3_document_id}.json",
        f"{test_update_config.embeddings_input}/{s3_document_id}_translated_en.json",
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
def test_s3_client(s3_bucket_and_region, test_s3_objects):
    with mock_aws():
        s3_client = S3Client(s3_bucket_and_region["region"])

        s3_client.client.create_bucket(
            Bucket=s3_bucket_and_region["bucket"],
            CreateBucketConfiguration={
                "LocationConstraint": s3_bucket_and_region["region"]
            },
        )

        for key in test_s3_objects:
            s3_client.client.put_object(
                Bucket=s3_bucket_and_region["bucket"],
                Key=key,
                Body=test_s3_objects[key],
            )

        yield s3_client


@pytest.fixture
def test_s3_client_filled_archive(
    test_update_config, s3_bucket_and_region, s3_document_id
):
    with mock_aws():
        s3_client = S3Client(s3_bucket_and_region["region"])

        s3_client.client.create_bucket(
            Bucket=s3_bucket_and_region["bucket"],
            CreateBucketConfiguration={
                "LocationConstraint": s3_bucket_and_region["region"]
            },
        )

        archive_keys = [
            f"{test_update_config.archive_prefix}/{test_update_config.parser_input}/{s3_document_id}/2022-01-21-01-12-12.json",
            f"{test_update_config.archive_prefix}/{test_update_config.parser_input}/{s3_document_id}/2023-01-21-01-12-12.json",
            f"{test_update_config.archive_prefix}/{test_update_config.parser_input}/{s3_document_id}/2022-01-23-01-12-12.json",
            f"{test_update_config.archive_prefix}/{test_update_config.parser_input}/{s3_document_id}/2022-01-21-01-11-12.json",
        ]

        for key in archive_keys:
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


@pytest.fixture
def test_updates(s3_document_id):
    updates = {
        s3_document_id: [
            {
                "type": "name",
                "db_value": "NEW NAME",
                "s3_value": "An example document name.",
            },
            {
                "type": "description",
                "db_value": "NEW DESCRIPTION",
                "s3_value": "An example document description.",
            },
            {
                "type": "source_url",
                "db_value": "https://www.NEW_SOURCE_URL.pdf",
                "s3_value": "https://domain/path/to/document.pdf",
            },
            {
                "type": "metadata",
                "db_value": {
                    "hazards": [],
                    "frameworks": [],
                    "instruments": [
                        "Capacity building|Governance",
                        "Education, training and knowledge dissemination|Information",
                    ],
                    "keywords": [
                        "Adaptation",
                        "Institutions / Administrative Arrangements",
                        "Research And Development",
                        "Energy Supply",
                        "Energy Demand",
                        "REDD+ And LULUCF",
                        "Transport",
                    ],
                    "sectors": ["Economy-wide", "Health", "Transport"],
                    "topics": ["Adaptation", "Mitigation"],
                },
                "s3_value": {},
            },
        ]
    }

    return [
        Update(
            type=UpdateTypes(update["type"]),
            s3_value=update["s3_value"],
            db_value=update["db_value"],
        )
        for update in updates[s3_document_id]
    ]


@pytest.fixture
def mock_cdn_config():
    """CDN bucket config to be used for non-aws based tests"""
    return {
        "bucket": "offline-unit-tests-cdn",
        "region": "eu-west-1",
    }


@pytest.fixture
def test_s3_client__cdn(mock_cdn_config):
    """Empty cdn and pipeline buckets to be used for non-aws based testing"""

    with mock_aws():
        s3_client = S3Client(mock_cdn_config["region"])

        s3_client.client.create_bucket(
            Bucket=mock_cdn_config["bucket"],
            CreateBucketConfiguration={"LocationConstraint": mock_cdn_config["region"]},
        )

        yield s3_client


@pytest.fixture
def pdf_bytes():
    """Bytes from a valid pdf"""
    fixture_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    pdf_data = os.path.join(fixture_dir, "sample.pdf")
    with open(pdf_data, "rb") as b:
        contents = b.read()
    return contents


@pytest.fixture
def doc_bytes():
    """Bytes from a valid doc"""
    fixture_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    doc_data = os.path.join(fixture_dir, "sample-for-word-to-pdf-conversion.doc")
    with open(doc_data, "rb") as b:
        contents = b.read()
    return contents
