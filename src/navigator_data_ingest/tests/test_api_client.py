import pytest
import requests

from navigator_data_ingest.base.api_client import upload_document


@pytest.mark.unit
def test_upload_document__readable(
    test_s3_client__cdn,
    mock_cdn_config,
    requests_mock,
    pdf_bytes,
):
    url = "mock://somedata.pdf"
    content_type = "application/pdf"

    session = requests.Session()
    requests_mock.get(url, content=pdf_bytes, headers={"content-type": content_type})

    result = upload_document(
        session=session,
        source_url=url,
        s3_prefix="TEST/1970",
        file_name_without_suffix="test_slug",
        document_bucket=mock_cdn_config["bucket"],
        import_id="TEST.0.1",
    )

    assert result.content_type == content_type
    assert result.cdn_object.startswith("TEST/1970/test_slug")
    assert result.cdn_object.endswith(".pdf")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "content_type", "data"),
    [
        ("mock://somedata", "text/html", b"<html></html>"),
    ],
)
def test_upload_document__unreadable(
    test_s3_client__cdn,
    mock_cdn_config,
    requests_mock,
    url,
    content_type,
    data,
):
    session = requests.Session()
    requests_mock.get(url, content=data, headers={"content-type": content_type})

    result = upload_document(
        session=session,
        source_url=url,
        s3_prefix="TEST/1970",
        file_name_without_suffix="test_slug",
        document_bucket=mock_cdn_config["bucket"],
        import_id="TEST.0.1",
    )

    assert result.md5_sum is None
    assert result.cdn_object is None
    assert result.content_type is None
