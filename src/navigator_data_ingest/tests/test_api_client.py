import contextlib

import pytest
import requests

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import IngestResult, IngestType
from navigator_data_ingest.base.api_client import (
    upload_document,
    _create_file_name_for_upload,
    _download_from_source,
    _store_document_in_cache,
    write_results_file,
)
from navigator_data_ingest.base.types import UnsupportedContentTypeError


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "input_content_type", "output_content_type", "output_extension"),
    [
        ("mock://somedata.pdf", "application/pdf", "application/pdf", ".pdf"),
        (
            "mock://somedata.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",  # type infered from file data
            ".pdf",
        ),
        ("mock://somedata.doc", "application/msword", "application/msword", ".pdf"),
        (
            "https://the-internet.herokuapp.com/dynamic_content",
            "text/html",
            "text/html",
            ".pdf",
        ),
        (
            "https://www.planalto.gov.br/ccivil_03/_ato2007-2010/2009/lei/l12187.htm",
            "text/html",
            "text/html",
            ".pdf",
        ),
    ],
)
def test_upload_document__readable(
    test_s3_client__cdn,
    mock_cdn_config,
    requests_mock,
    pdf_bytes,
    doc_bytes,
    html_bytes,
    url,
    input_content_type,
    output_content_type,
    output_extension,
    monkeypatch,
):
    session = requests.Session()

    content_type_response_mapping = {
        "application/pdf": pdf_bytes,
        # TODO: we could also specify docx_bytes, but this doesn't really matter for this
        # test as the conversion will work regardless of doc or docx
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": doc_bytes,
        "application/msword": doc_bytes,
        "text/html": html_bytes,
    }

    content = content_type_response_mapping[input_content_type]

    requests_mock.get(
        url, content=content, headers={"content-type": input_content_type}
    )

    result = upload_document(
        session=session,
        source_url=url,
        s3_prefix="TEST/1970",
        file_name_without_suffix="test_slug",
        document_bucket=mock_cdn_config["bucket"],
        import_id="TEST.0.1",
    )

    assert result.content_type == output_content_type
    assert result.cdn_object is not None
    assert result.cdn_object.startswith("TEST/1970/test_slug")
    assert result.cdn_object.endswith(output_extension)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "input_content_type"),
    [
        ("mock://somedata.octet", "binary/octet-stream"),
        ("mock://somedata.zip", "application/zip"),
    ],
)
def test_upload_document__unsupported_content_type(
    test_s3_client__cdn,
    mock_cdn_config,
    requests_mock,
    url,
    input_content_type,
):
    session = requests.Session()

    requests_mock.get(
        url, content=b"mock content", headers={"content-type": input_content_type}
    )

    with pytest.raises(UnsupportedContentTypeError):
        upload_document(
            session=session,
            source_url=url,
            s3_prefix="TEST/1970",
            file_name_without_suffix="test_slug",
            document_bucket=mock_cdn_config["bucket"],
            import_id="TEST.0.1",
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "file_hash,file_name_without_suffix,file_suffix,s3_prefix,expected_output",
    [
        (
            "abc123def456",
            "test_document",
            ".pdf",
            "files/2024",
            "files/2024/test_document_abc123def456.pdf",
        ),
        (
            "def456ghi789",
            "a"
            * 250,  # 250 characters - should be trimmed to 200 by first trimming logic
            ".pdf",
            "PROD/2025",
            f"PROD/2025/{'a' * 200}_def456ghi789.pdf",
        ),
        (
            "789xyz123abc",
            "National_Climate_Change_Adaptation_Strategy_and_Implementation_Plan_for_Sustainable_Development_Goals_Integration_and_Resilience_Building_Across_Multiple_Sectors_Including_Agriculture_Water_Energy",
            ".pdf",
            "files/2023/Q4",
            "files/2023/Q4/National_Climate_Change_Adaptation_Strategy_and_Implementation_Plan_for_Sustainable_Development_Goals_Integration_and_Resilience_Building_Across_Multiple_Sectors_Including_Agriculture_Water_Energy_789xyz123abc.pdf",
        ),
        (
            "1234567890abcdef1234567890abcdef",  # 32 char hash (typical md5)
            "B" * 200,  # 200 characters after first trim
            ".pdf",
            "very/long/prefix/path/"
            + "A" * 800,  # Very long prefix to trigger second trimming
            f"very/long/prefix/path/{'A' * 800}/{'B' * 164}_1234567890abcdef1234567890abcdef.pdf",
        ),
    ],
)
def test_create_file_name_for_upload(
    file_hash, file_name_without_suffix, file_suffix, s3_prefix, expected_output
):
    result = _create_file_name_for_upload(
        file_hash, file_name_without_suffix, file_suffix, s3_prefix
    )

    assert result == expected_output
    assert result.startswith(s3_prefix)
    assert file_hash in result
    assert result.endswith(file_suffix)
    # Ensure the full path never exceeds S3's 1024 byte limit
    assert len(result.encode("utf-8")) <= 1024


@pytest.mark.unit
@pytest.mark.parametrize(
    "status_code,expectation",
    [
        (200, contextlib.nullcontext()),
        (
            500,
            pytest.raises(
                Exception,
                match="500 Server Error: None for url: mock://test-document.pdf",
            ),
        ),
    ],
)
def test_download_from_source(requests_mock, status_code, expectation):
    session = requests.Session()
    url = "mock://test-document.pdf"
    expected_content = b"test pdf content"

    requests_mock.get(url, content=expected_content, status_code=status_code)

    with expectation:
        response = _download_from_source(session, url)
        assert response.status_code == 200
        assert response.content == expected_content


@pytest.mark.unit
def test_store_document_in_cache(test_s3_client__cdn, mock_cdn_config):
    bucket = mock_cdn_config["bucket"]
    name = "test_folder/test_file.pdf"
    data = b"test document data"

    result = _store_document_in_cache(bucket, name, data)

    assert result == name.lstrip("/")
    assert "test_file.pdf" in result


@pytest.mark.unit
def test_write_results_file(test_s3_client__cdn, mock_cdn_config):
    bucket = mock_cdn_config["bucket"]
    input_file_path = S3Path(f"s3://{bucket}/test_input/some_file.json")
    results = [IngestResult(document_id="TEST.1.1", type=IngestType.new, error=None)]

    write_results_file(input_file_path, results)

    expected_file = input_file_path.parent / "reports" / "ingest" / "batch_1.json"
    assert expected_file.exists()
