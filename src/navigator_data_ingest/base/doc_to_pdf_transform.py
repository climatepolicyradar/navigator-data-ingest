from pathlib import Path
from tempfile import mkdtemp

import subprocess

def transform_docx_to_pdf(file_content: str) -> str:
    """
    Transforms a docx file into pdf
    
    It uses a temporary directory to store the docx and pdf files.

    :param file_content: the loaded file content of the docx file
    :return: the file content of the pdf file
    """
    worker_dir = mkdtemp(prefix="worker_")

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
    
    return pdf_file_content