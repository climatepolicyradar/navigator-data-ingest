"""A simple API client for creating documents & associations."""
import hashlib
import json
import logging
from typing import cast
from datetime import datetime

import requests
from cloudpathlib import CloudPath, S3Path
from cpr_sdk.parser_models import ParserInput
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential

from navigator_data_ingest.base.pdf_conversion import (
    convert_doc_to_pdf,
    capture_pdf_and_get_content_type_from_url,
    add_last_page_watermark,
    generate_watermark_text,
)
from navigator_data_ingest.base.types import (
    CONTENT_TYPE_DOC,
    CONTENT_TYPE_DOCX,
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    UnsupportedContentTypeError,
    UploadResult,
    IngestResult,
)
from navigator_data_ingest.base.utils import determine_content_type

_LOGGER = logging.getLogger(__file__)

META_KEY = "metadata"

REQUEST_HEADERS = {
    "User-Agent": "Climate Policy Radar Data Ingestion Service",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}


def upload_document(
    session: requests.Session,
    source_url: str,
    s3_prefix: str,
    file_name_without_suffix: str,
    document_bucket: str,
    import_id: str,
) -> UploadResult:
    """
    Upload a document to the cloud, and returns the cloud URL.

    The remote document will have the specified file_name_without_suffix_{md5_hash},
    where md5_hash is the hash of the file and the suffix is determined from the content type.
    `file_name_without_suffix` will be trimmed if the total path length exceeds 1024 bytes,
    which is the S3 maximum path length.

    :param requests.Session session: The session used for making the request.
    :return DocumentUploadResult: the remote URL and the md5_sum of its contents
    :raises UnsupportedContentTypeError: if the content is of type multi-file or not within the list of supported
    """
    _LOGGER.info(f"Downloading document from '{source_url}' for {import_id}")

    # Download and determine content type
    download_response = _download_from_source(session, source_url)
    content_type = determine_content_type(download_response, source_url)
    file_content = download_response.content

    watermark_text = generate_watermark_text(source_url, datetime.now())

    # Convert to PDF based on content type
    if content_type == CONTENT_TYPE_HTML:
        # If the content type is HTML or we don't know it from a requests.get,
        # capture the PDF from the URL
        _LOGGER.info(f"Capturing PDF from URL with HTML content type: '{source_url}'")
        file_content, content_type = capture_pdf_and_get_content_type_from_url(
            source_url
        )
        file_content = add_last_page_watermark(
            file_content,
            watermark_text,
        )
    elif content_type in {CONTENT_TYPE_DOCX, CONTENT_TYPE_DOC}:
        # If the content type is DOCX or DOC, convert it to PDF
        _LOGGER.info(f"Converting DOCX or DOC to PDF: '{source_url}'")
        file_content = convert_doc_to_pdf(file_content)
        file_content = add_last_page_watermark(
            file_content,
            watermark_text,
        )
    elif content_type == CONTENT_TYPE_PDF:
        # If the content type is PDF, we can use the original file content
        pass
    else:
        raise UnsupportedContentTypeError(content_type)

    # Calculate the md5sum
    file_hash = hashlib.md5(file_content).hexdigest()
    file_suffix = ".pdf"

    file_name = _create_file_name_for_upload(
        file_hash, file_name_without_suffix, file_suffix, s3_prefix
    )

    _LOGGER.info(
        f"Uploading supported single file document content from '{source_url}' "
        f"to CDN s3 bucket with filename '{file_name}'"
    )
    cdn_object = _store_document_in_cache(document_bucket, file_name, file_content)

    return UploadResult(
        cdn_object=cdn_object,
        md5_sum=file_hash,
        content_type=content_type,
    )


def _create_file_name_for_upload(
    file_hash: str, file_name_without_suffix: str, file_suffix: str, s3_prefix: str
) -> str:
    """Constructs a trimmed and enriched file name for uploading to S3"""
    # ext4 used in Amazon Linux /tmp directory has a max filename length of
    # 255 bytes, so trim to ensure we don't exceed that. Choose 240 initially to
    # allow for suffix.
    file_name_max_fs_len_no_suffix = file_name_without_suffix[:200]
    while len(file_name_max_fs_len_no_suffix.encode("utf-8")) > 200:
        file_name_max_fs_len_no_suffix = file_name_max_fs_len_no_suffix[:-5]

    # s3 can only handle paths of up to 1024 bytes. To ensure we don't exceed that,
    # we trim the filename if it's too long
    file_name_max_len = (
        1024
        - len(s3_prefix)
        - len(file_suffix)
        - len(file_hash)
        - len("_.")  # length of additional characters for joining path components
    )
    file_name_no_suffix_trimmed = file_name_max_fs_len_no_suffix[:file_name_max_len]
    # Safe not to loop over the encoding of file_name because everything we're
    # adding is 1byte == 1char
    file_name = f"{s3_prefix}/{file_name_no_suffix_trimmed}_{file_hash}{file_suffix}"

    return file_name


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def _download_from_source(
    session: requests.Session, source_url: str, timeout: int = 30
) -> requests.Response:
    # Try the orginal source url
    download_response = session.get(
        source_url, allow_redirects=True, timeout=timeout, headers=REQUEST_HEADERS
    )

    # TODO this is a hack and we should handle source urls upstream in the backend
    if download_response.status_code == 404:
        # mutation 1 - remove %
        download_response = session.get(
            source_url.replace("%", ""),
            allow_redirects=True,
            timeout=timeout,
            headers=REQUEST_HEADERS,
        )

    if download_response.status_code == 404:
        # mutation 2 - replace % with the encoded version, i.e. %25
        download_response = session.get(
            source_url.replace("%", "%25"),
            allow_redirects=True,
            timeout=timeout,
            headers=REQUEST_HEADERS,
        )

    if download_response.status_code >= 300:
        raise Exception(
            f"Downloading source document failed: {download_response.status_code} "
            f"{download_response.text}"
        )
    return download_response


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
)
def _store_document_in_cache(
    bucket: str,
    name: str,
    data: bytes,
) -> str:
    clean_name = name.lstrip("/")
    output_file_location = S3Path(f"s3://{bucket}/navigator/{clean_name}")
    with output_file_location.open("wb") as output_file:
        output_file.write(data)
    return clean_name


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def save_errors(
    bucket: str,
    name: str,
    data: bytes,
) -> str:
    clean_name = name.lstrip("/")
    output_file_location = S3Path(f"s3://{bucket}/{clean_name}")
    with output_file_location.open("wb") as output_file:
        output_file.write(data)
    return clean_name


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
)
def write_parser_input(
    output_location: CloudPath,
    parser_input: ParserInput,
) -> None:
    output_file_location = cast(
        S3Path,
        output_location / f"{parser_input.document_id}.json",
    )
    with output_file_location.open("w") as output_file:
        output_file.write(parser_input.model_dump_json(indent=2))


@retry(
    stop=stop_after_attempt(2),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
)
def write_results_file(input_file_path: CloudPath, results: list[IngestResult]):
    path: CloudPath = input_file_path.parent / "reports" / "ingest" / "batch_1.json"

    _LOGGER.info(
        "Writing results.",
        extra={
            "props": {
                "count": len(results),
                "path": str(path),
            }
        },
    )
    with path.open("w") as output_file:
        output_file.write(
            json.dumps([r.model_dump(mode="json") for r in results], indent=2)
        )
