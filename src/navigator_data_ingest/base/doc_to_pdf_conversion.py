from pathlib import Path
import shutil
from tempfile import mkdtemp
from uuid import uuid4

import subprocess

def convert_doc_to_pdf(file_content: str) -> str:
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