from pathlib import Path
import shutil
from tempfile import mkdtemp
from uuid import uuid4
import subprocess
from io import BytesIO
from datetime import datetime

from playwright.sync_api import sync_playwright
import fitz

PLAYWRIGHT_REQUEST_TIMEOUT_SECONDS = 60


def convert_doc_to_pdf(file_content: bytes) -> bytes:
    """
    Transforms a docx / doc file into pdf, and returns the bytes file content

    It uses a temporary directory to store the docx and pdf files.

    :param file_content: the loaded file content of the docx file
    :return: the file content of the pdf file
    """
    suffix = str(uuid4())
    # to make this thread safe, create a unique directory for each worker
    worker_dir = mkdtemp(prefix="worker_", suffix=suffix)

    input_file_path = f"{worker_dir}/doc.docx"
    output_file_path = f"{worker_dir}/doc.pdf"

    with open(input_file_path, "wb") as input_file:
        input_file.write(file_content)

    cmd = [
        "soffice",
        "--headless",
        "--convert-to",
        "pdf",
        "--outdir",
        str(Path(output_file_path).parent),
        input_file_path,
    ]

    process = subprocess.run(cmd, capture_output=True, text=True)
    if process.returncode != 0:
        raise RuntimeError(f"Conversion failed: {process.stderr}")

    with open(output_file_path, "rb") as output_file:
        pdf_file_content = output_file.read()

    shutil.rmtree(worker_dir)
    return pdf_file_content


def capture_pdf_from_url(url: str) -> bytes:
    """
    Capture a PDF from a URL using Playwright.

    Args:
        url: The URL of the page to capture.
        output_path: The path to save the PDF file.
    """

    with sync_playwright() as p:
        browser = p.chromium.launch()

        page = browser.new_page()

        page.goto(url, timeout=1000 * PLAYWRIGHT_REQUEST_TIMEOUT_SECONDS)  # ms
        page.wait_for_load_state(
            "networkidle", timeout=1000 * PLAYWRIGHT_REQUEST_TIMEOUT_SECONDS
        )
        pdf_bytes = page.pdf(
            format="A4",
            margin={"bottom": "10mm", "top": "10mm", "right": "10mm", "left": "10mm"},
        )

        browser.close()

        return pdf_bytes


def generate_watermark_text(
    url: str,
    date: datetime,
) -> str:
    """Generate a watermark text for a PDF."""

    # Format date as "01 January 2023"
    date_str = date.strftime("%d %B %Y")

    return f"""Original publicly accessible source: {url}.

This PDF was created by Climate Policy Radar (climatepolicyradar.org) on {date_str}.

For non-commercial use only. Reach out to us at support@climatepolicyradar.org if you have any enquiries."""


def add_last_page_watermark(
    pdf_bytes: bytes,
    watermark_text: str,
) -> bytes:
    """
    Add a new page at the end of a PDF with watermark text.

    Used for PDFs we've converted from other formats.
    """

    pdf_stream = BytesIO(pdf_bytes)
    doc: fitz.Document = fitz.open(stream=pdf_stream, filetype="pdf")

    # Create a new page with the same dimensions as the last page
    page = doc.new_page(pno=-1, width=doc[-1].rect.width, height=doc[-1].rect.height)

    # Calculate text rectangle with margins
    margin = 72  # 1 inch margin in points
    text_rect = fitz.Rect(
        margin, margin, page.rect.width - margin, page.rect.height - margin
    )

    # Insert text with word-wrapping
    page.insert_textbox(
        text_rect,
        watermark_text,
        fontname="helv",
        fontsize=12,
        align=0,
    )

    output_stream = BytesIO()
    doc.save(output_stream)
    doc.close()

    return output_stream.getvalue()
