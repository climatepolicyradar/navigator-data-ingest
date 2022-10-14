import json
import logging
from concurrent.futures import as_completed, Executor
from typing import Generator, Iterable

import requests
from cloudpathlib import S3Path
from slugify import slugify

from navigator_data_ingest.base.types import (
    Document,
    DocumentGenerator,
    DocumentParserInput,
    DocumentUploadResult,
)
from navigator_data_ingest.base.api_client import upload_document

_LOGGER = logging.getLogger(__file__)


class LawPolicyGenerator(DocumentGenerator):
    """A generator of validated Document objects for inspection & upload"""

    def __init__(self, input_file: S3Path):
        self._input_file = input_file

    def process_source(self) -> Generator[Document, None, None]:
        """Generate documents for processing from the configured source."""

        with self._input_file.open("r") as input:
            json_docs = json.load(input)

        for d in json_docs:
            yield (Document(**d))


def handle_all_documents(
    executor: Executor,
    source: Iterable[Document],
    document_bucket: str,
) -> Generator[DocumentParserInput, None, None]:
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
            document_upload_result = future.result()
        except Exception:
            _LOGGER.exception(
                f"Handling the following document generated an exception: {document}"
            )
        else:
            # inputs are returned for documents that have not been previously handled
            if document_upload_result is not None:
                _LOGGER.info(f"Uploaded content for '{document}'")
                _LOGGER.info(f"Writing parser input for '{document.import_id}")
                url_for_parser = document_upload_result.cloud_url or document.source_url
                document.url = url_for_parser
                document.md5_sum = document_upload_result.md5_sum
                yield DocumentParserInput(
                    document_name=document.name,
                    document_description=document.description,
                    document_url=url_for_parser,
                    document_id=document.import_id,
                    document_content_type=document_upload_result.content_type,
                    document_detail=document,
                )

    _LOGGER.info("Done uploading documents")


def _upload_document(
    session: requests.Session,
    document: Document,
    document_bucket: str,
) -> DocumentUploadResult:
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
            f"Skipping upload for '{document.source}:{document.import_id}:"
            f"{document.name}' because the source URL is empty"
        )
        return DocumentUploadResult(
            cloud_url=None,
            md5_sum=None,
            content_type=None,
        )

    clean_url = document.source_url.split("|")[0].strip()

    return upload_document(
        session,
        clean_url,
        file_name,
        document_bucket,
    )


def _handle_document(
    document: Document,
    document_bucket: str,
) -> DocumentUploadResult:
    """
    Upload document source files & update details via API endpoing.

    :param Document document: A document to upload.
    """
    _LOGGER.info(f"Handling document: {document}")

    session = requests.Session()
    try:
        uploaded_document_result = _upload_document(
            session,
            document,
            document_bucket,
        )

        # TODO: (BAK-1208) Send updated md5sum/url details to API endpoint
        # update_document_response = post_update(session=session, document=document)
        # if update_document_response.status_code >= 300:
        #     # TODO: More nuanced status response handling
        #     _LOGGER.error(
        #         f"Failed to update entry in the database for '{document.import_id}': "
        #         f"{update_document_response.text}"
        #     )

        return uploaded_document_result
    except Exception:
        _LOGGER.exception(f"Uploading document with URL {document.source_url} failed")
        raise
