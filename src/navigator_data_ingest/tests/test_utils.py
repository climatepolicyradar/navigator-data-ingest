import json

from cloudpathlib import S3Path
from requests import Response
import pytest

from navigator_data_ingest.base.types import (
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    CONTENT_TYPE_DOC,
    CONTENT_TYPE_DOCX,
)
from navigator_data_ingest.base.utils import (
    LawPolicyGenerator,
    determine_content_type,
    read_s3_json_file,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("file_content", "source_url", "content_type_header", "expected"),
    (
        # Stage 1: Magic bytes detection (highest priority)
        # PDF magic bytes - detected regardless of URL extension or header
        (
            b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n",
            "https://example.com/document",
            "",
            CONTENT_TYPE_PDF,
        ),
        (
            b"%PDF-1.7\n1 0 obj\n<<\n/Type /Catalog\n>>",
            "https://example.com/file.html",  # Wrong extension
            "text/html",  # Wrong header
            CONTENT_TYPE_PDF,  # Magic bytes win
        ),
        # Stage 2: File extension fallback
        # No magic bytes detected, infer from extension
        (
            b"",  # Empty content
            "https://example.com/document.pdf",
            "",
            CONTENT_TYPE_PDF,
        ),
        # HTML files - unsupported from detection so fall back to extension
        (
            b"<!DOCTYPE html>\n<html><head><title>Test</title></head></html>",
            "https://example.com/page.html",
            "",
            CONTENT_TYPE_HTML,
        ),
        (
            b"<html><head></head><body><h1>Content</h1></body></html>",
            "https://example.com/document.html",
            "",
            CONTENT_TYPE_HTML,
        ),
        (
            b"Plain content",
            "https://example.com/document.docx",
            "",
            CONTENT_TYPE_DOCX,
        ),
        # Old MS Word DOC files - unsupported from detection so fall back to extension
        (
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00>\x00\x03\x00\xfe\xff\t\x00\x06\x00",
            "https://example.com/document.doc",
            "",
            CONTENT_TYPE_DOC,
        ),
        # Stage 3: Content-Type header fallback
        # No magic bytes, no extension, use header
        (
            b"",
            "https://example.com/document",
            "application/pdf",
            CONTENT_TYPE_PDF,
        ),
        # HTML content without extension falls back to header
        (
            b"<!DOCTYPE html>\n<html><body><h1>Test</h1></body></html>",
            "https://example.com/page",
            "text/html",
            CONTENT_TYPE_HTML,
        ),
        (
            b"Plain text",
            "https://example.com/file",
            "text/html",
            CONTENT_TYPE_HTML,
        ),
        # DOC format with real signature but no extension - falls back to header
        (
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00\x00\x00\x00\x00\x00\x00\x00",
            "https://example.com/file",
            "application/msword",
            CONTENT_TYPE_DOC,
        ),
        # Edge cases
        # Header with charset parameter (should be stripped)
        (
            b"",
            "https://example.com/page",
            "text/html; charset=utf-8",
            CONTENT_TYPE_HTML,
        ),
        # No magic bytes, no extension, no header
        (
            b"Just some text",
            "https://example.com/file",
            "",
            "",
        ),
        # Unknown extension, no magic bytes, no header
        (
            b"Plain text",
            "https://example.com/file.xyz",
            "",
            "",
        ),
    ),
)
def test_determine_content_type(
    file_content, source_url, content_type_header, expected
):
    """Test the three-stage content type detection: magic bytes -> extension -> header."""
    test_response = Response()
    test_response._content = file_content
    test_response.headers["Content-Type"] = content_type_header

    result = determine_content_type(test_response, source_url)
    assert result == expected


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
