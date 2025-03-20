import re
import pytest
import fitz

from Levenshtein import distance

from navigator_data_ingest.base.doc_to_pdf_conversion import convert_doc_to_pdf


def all_text(doc: fitz.Document) -> str:
    text = ""
    for page in doc:
        text += re.sub("( |\n|\t)+", " ", page.get_text().replace("\n", " "))
    return text


@pytest.mark.skip(reason="This test requires libreoffice in the environment")
def test_convert_doc_to_pdf():
    with open(
        "src/navigator_data_ingest/tests/fixtures/sampe-01-for-conversion.doc", "rb"
    ) as file:
        doc_content = file.read()

    pdf_content = convert_doc_to_pdf(doc_content)

    converted_pdf = fitz.open(stream=pdf_content, filetype="pdf")
    expected_pdf = fitz.open(
        "src/navigator_data_ingest/tests/fixtures/sampe-01-for-conversion.pdf"
    )

    # libreoffice messes with the pages -- the converted pdf is actually longer (67 pages vs 54)
    # assert converted_pdf.page_count == expected_pdf.page_count
    # this also results in content change: e.g. the page numbers are not the same. Tables in particular are dragged out
    # into multiple pages on the conversion

    assert (
        distance(all_text(converted_pdf), all_text(expected_pdf))
        / len(all_text(expected_pdf))
        < 0.06
    )
    assert (len(all_text(converted_pdf)) - len(all_text(expected_pdf))) / len(
        all_text(expected_pdf)
    ) < 0.01
