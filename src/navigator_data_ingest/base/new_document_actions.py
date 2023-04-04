import logging
import traceback
from concurrent.futures import as_completed, Executor
from typing import Generator, Iterable

import requests
from slugify import slugify

from navigator_data_ingest.base.api_client import (
    upload_document,
    update_document_details,
)
from navigator_data_ingest.base.types import (
    Document,
    DocumentParserInput,
    UploadResult,
    HandleResult,
)

_LOGGER = logging.getLogger(__file__)


def handle_new_documents(
    executor: Executor,
    source: Iterable[Document],
    document_bucket: str,
) -> Generator[HandleResult, None, None]:
    """
    Handle all documents.

    For each document:
      - Upload doc.source_url to cloud storage & set doc.url.
      - Set doc.content_type to appropriate value.

    TODO: appropriately handle complex multi-file documents

    The remote filename follows the template on
    https://www.notion.so/climatepolicyradar/Document-names-on-S3-6f3cd748c96141d3b714a95b42842aeb
    """
    tasks = {
        executor.submit(
            _handle_document,
            document,
            document_bucket,
        ): document
        for document in source
    }

    for future in as_completed(tasks):
        # check result, handle errors & shut down
        document = tasks[future]
        try:
            handle_result = future.result()
        except Exception:
            _LOGGER.exception(
                f"Handling document '{document.import_id}' generated an "
                "unexpected exception"
            )
        else:
            yield handle_result

    _LOGGER.info("Done uploading documents")


def _upload_document(
    session: requests.Session,
    document: Document,
    document_bucket: str,
) -> UploadResult:
    """
    Upload a single document.

    :param requests.Session session: The session to use for HTTP requests
    :param Document document: The document description
    :return DocumentUploadResult: Details of the document content & upload location
    """
    doc_slug = slugify(document.name)
    doc_geo = document.geography
    doc_year = document.publication_ts.year
    file_name = f"{doc_geo}/{doc_year}/{doc_slug}"

    if not document.source_url:
        _LOGGER.info(
            f"Skipping upload for '{document.source}:{document.document_id}:"
            f"{document.name}' because the source URL is empty"
        )
        return UploadResult(
            cdn_object=None,
            md5_sum=None,
            content_type=None,
        )

    clean_url = document.source_url.split("|")[0].strip()

    return upload_document(
        session, clean_url, file_name, document_bucket, document.document_id
    )


def _handle_document(
    document: Document,
    document_bucket: str,
) -> HandleResult:
    """
    Upload document source files & update details via API endpoint.

    :param Document document: A document to upload.
    """
    _LOGGER.info(f"Handling document: {document}")

    session = requests.Session()
    parser_input = DocumentParserInput(
        document_id=document.document_id,
        document_slug=document.slug,
        document_name=document.name,
        document_description=document.description,
        document_source_url=document.source_url,
        document_metadata=document,
    )

    try:
        uploaded_document_result = _upload_document(
            session,
            document,
            document_bucket,
        )
        _LOGGER.info(f"Uploaded content for '{document.document_id}'")

    except Exception:
        _LOGGER.exception(f"Ingesting document with ID '{document.document_id}' failed")
        return HandleResult(error=traceback.format_exc(), parser_input=parser_input)

    parser_input = parser_input.copy(
        update={
            "document_cdn_object": uploaded_document_result.cdn_object,
            "document_content_type": uploaded_document_result.content_type,
            "document_md5_sum": uploaded_document_result.md5_sum,
        },
    )

    try:
        update_document_details(
            session,
            document.document_id,
            uploaded_document_result,
        )
        _LOGGER.info(f"Updating details for '{document.document_id}")
    except Exception:
        _LOGGER.exception(f"Ingesting document with ID '{document.document_id}' failed")
        return HandleResult(error=traceback.format_exc(), parser_input=parser_input)

    return HandleResult(parser_input=parser_input)
