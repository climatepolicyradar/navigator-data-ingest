import re
import fitz
from datetime import datetime
from Levenshtein import distance

from navigator_data_ingest.base.pdf_conversion import (
    convert_doc_to_pdf,
    add_last_page_watermark,
    generate_watermark_text,
)


def all_text(doc: fitz.Document) -> str:
    text = ""
    for page in doc:
        text += re.sub("( |\n|\t)+", " ", page.get_text().replace("\n", " "))
    return text


def test_convert_doc_to_pdf():
    with open(
        "src/navigator_data_ingest/tests/fixtures/sample-for-word-to-pdf-conversion.doc",
        "rb",
    ) as file:
        doc_content = file.read()

    pdf_content = convert_doc_to_pdf(doc_content)

    converted_pdf = fitz.open(stream=pdf_content, filetype="pdf")
    expected_pdf = fitz.open(
        "src/navigator_data_ingest/tests/fixtures/sample-for-word-to-pdf-conversion.pdf"
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


def test_add_last_page_watermark():
    with open(
        "src/navigator_data_ingest/tests/fixtures/sample-for-word-to-pdf-conversion.pdf",
        "rb",
    ) as file:
        pdf_content = file.read()

    watermark_text = generate_watermark_text(
        "https://example.com",
        datetime.now(),
    )

    watermarked_pdf_content = add_last_page_watermark(pdf_content, watermark_text)

    original_pdf = fitz.open(stream=pdf_content, filetype="pdf")

    watermarked_pdf = fitz.open(stream=watermarked_pdf_content, filetype="pdf")
    assert watermarked_pdf.page_count == original_pdf.page_count + 1
    assert watermarked_pdf[-1].get_text().strip().replace(
        "\n", " "
    ) == watermark_text.strip().replace("\n", " ")
