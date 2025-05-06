import pytest
import requests

from navigator_data_ingest.base.api_client import upload_document


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "input_content_type", "output_content_type", "output_extension"),
    [
        ("mock://somedata.pdf", "application/pdf", "application/pdf", ".pdf"),
        (
            "mock://somedata.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".pdf",
        ),
        ("mock://somedata.doc", "application/msword", "application/msword", ".pdf"),
        (
            "https://the-internet.herokuapp.com/dynamic_content",
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

    result = upload_document(
        session=session,
        source_url=url,
        s3_prefix="TEST/1970",
        file_name_without_suffix="test_slug",
        document_bucket=mock_cdn_config["bucket"],
        import_id="TEST.0.1",
    )

    assert result.content_type == input_content_type
    assert result.cdn_object is None
    assert result.md5_sum is None
