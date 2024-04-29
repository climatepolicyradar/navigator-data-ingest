from requests import Response
import pytest

from navigator_data_ingest.base.types import CONTENT_TYPE_HTML, CONTENT_TYPE_PDF
from navigator_data_ingest.base.utils import determine_content_type


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
    )
)
def test_determine_content_type(content_type, source_url, want):
    test_response = Response()
    test_response.headers["Content-Type"] = content_type

    got = determine_content_type(test_response, source_url)
    assert got == want
