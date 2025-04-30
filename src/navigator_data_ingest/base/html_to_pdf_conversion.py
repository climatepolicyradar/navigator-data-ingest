from playwright.sync_api import sync_playwright


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

        page.goto(url, timeout=1000 * 60 * 60)  # ms
        page.wait_for_load_state("networkidle", timeout=0)
        pdf_bytes = page.pdf(
            format="A4",
            margin={"bottom": "10mm", "top": "10mm", "right": "10mm", "left": "10mm"},
        )

        browser.close()

        return pdf_bytes
