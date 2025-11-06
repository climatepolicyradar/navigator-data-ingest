import json

from cloudpathlib import S3Path
from requests import Response
import pytest

from navigator_data_ingest.base.types import CONTENT_TYPE_HTML, CONTENT_TYPE_PDF
from navigator_data_ingest.base.utils import (
    LawPolicyGenerator,
    determine_content_type,
    read_s3_json_file,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content_type", "source_url", "want"),
    (
        ["text/html", "https://aweb.site/file", CONTENT_TYPE_HTML],
        ["text/html", "https://aweb.site/file.pdf", CONTENT_TYPE_PDF],
        ["application/pdf", "https://aweb.site/file", CONTENT_TYPE_PDF],
        ["application/pdf", "https://aweb.site/file.pdf", CONTENT_TYPE_PDF],
        ["", "https://aweb.site/file.pdf", CONTENT_TYPE_PDF],
        ["", "https://aweb.site/file", ""],
    ),
)
def test_determine_content_type(content_type, source_url, want):
    test_response = Response()
    test_response.headers["Content-Type"] = content_type

    got = determine_content_type(test_response, source_url)
    assert got == want


@pytest.mark.unit
def test_read_s3_json_file(test_s3_client__cdn, mock_cdn_config):
    bucket = mock_cdn_config["bucket"]
    test_data = {"key": "value", "number": 123}
    s3_path = S3Path(f"s3://{bucket}/test_file.json")

    with s3_path.open("w") as f:
        json.dump(test_data, f)

    result = read_s3_json_file(s3_path)

    assert result == test_data


@pytest.mark.unit
def test_law_policy_generator_process_new_documents(
    test_s3_client__cdn, mock_cdn_config
):
    bucket = mock_cdn_config["bucket"]
    input_data = {
        "new_documents": [
            {
                "publication_ts": "2013-01-01T00:00:00",
                "name": "Test Doc 1",
                "description": "Description 1",
                "source_url": "https://example.com/doc1.pdf",
                "download_url": None,
                "url": None,
                "md5_sum": None,
                "type": "Policy",
                "source": "CCLW",
                "import_id": "TEST.executive.1.1",
                "family_import_id": "TEST.family.1.0",
                "category": "Policy",
                "geography": "USA",
                "languages": ["English"],
                "metadata": {},
                "slug": "test_slug_1",
                "family_slug": "test_family_slug",
            }
        ],
        "updated_documents": {},
    }

    input_file = S3Path(f"s3://{bucket}/input/test_input.json")
    with input_file.open("w") as f:
        json.dump(input_data, f)

    output_location = S3Path(f"s3://{bucket}/output")
    generator = LawPolicyGenerator(input_file, output_location)

    new_docs = list(generator.process_new_documents())

    assert len(new_docs) == 1
    assert new_docs[0].import_id == "TEST.executive.1.1"


@pytest.mark.unit
def test_law_policy_generator_process_updated_documents(
    test_s3_client__cdn, mock_cdn_config
):
    bucket = mock_cdn_config["bucket"]
    input_data = {
        "new_documents": [],
        "updated_documents": {
            "TEST.executive.1.1": [
                {"type": "name", "s3_value": "Old Name", "db_value": "New Name"}
            ]
        },
    }

    input_file = S3Path(f"s3://{bucket}/input/test_input.json")
    with input_file.open("w") as f:
        json.dump(input_data, f)

    output_location = S3Path(f"s3://{bucket}/output")
    generator = LawPolicyGenerator(input_file, output_location)

    updated_docs = list(generator.process_updated_documents())

    assert len(updated_docs) == 1
    assert updated_docs[0][0] == "TEST.executive.1.1"
    assert len(updated_docs[0][1]) == 1
