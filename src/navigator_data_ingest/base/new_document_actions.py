import logging
import traceback

import pydantic
import requests
from cpr_sdk.parser_models import ParserInput
from cpr_sdk.pipeline_general_models import BackendDocument
from slugify import slugify

from navigator_data_ingest.base.api_client import upload_document
from navigator_data_ingest.base.types import (
    HandleResult,
    UploadResult,
)

_LOGGER = logging.getLogger(__file__)


def _upload_document(
    session: requests.Session,
    document: BackendDocument,
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
    s3_prefix = f"{doc_geo}/{doc_year}"

    if not document.download_url:
        if not document.source_url:
            _LOGGER.info(
                f"Skipping upload for '{document.source}:{document.import_id}:"
                f"{document.name}' because both the source URL and download URL are empty."
            )
            return UploadResult(
                cdn_object=None,
                md5_sum=None,
                content_type=None,
            )
        file_download_source = document.source_url
    else:
        file_download_source = document.download_url

    return upload_document(
        session,
        file_download_source,
        s3_prefix,
        doc_slug,
        document_bucket,
        document.import_id,
    )


def new_document(
    document: BackendDocument,
    document_bucket: str,
) -> HandleResult:
    """
    Upload document source files & update details via API endpoint.

    Function validates the source url in particular as in the ParserInput object we
    require that this is a valid AnyHttpUrl. All the other fields are direct
    translations from the BackendDocument object.

    :param Document document: A document to upload.
    """
    _LOGGER.info(f"Handling document: {document}")

    session = requests.Session()

    try:
        document_source_url = (
            pydantic.AnyHttpUrl(document.source_url) if document.source_url else None
        )

    except pydantic.ValidationError:
        _LOGGER.exception(f"Ingesting document with ID '{document.import_id}' failed.")
        return HandleResult(
            error=traceback.format_exc(),
            parser_input=ParserInput(
                document_id=document.import_id,
                document_slug=document.slug,
                document_name=document.name,
                document_description=document.description,
                document_source_url=None,
                document_metadata=document,
            ),
        )

    parser_input = ParserInput(
        document_id=document.import_id,
        document_slug=document.slug,
        document_name=document.name,
        document_description=document.description,
        document_source_url=document_source_url,
        document_metadata=document,
    )

    try:
        uploaded_document_result = _upload_document(
            session,
            document,
            document_bucket,
        )
        _LOGGER.info(f"Uploaded content for '{document.import_id}'")

    except Exception:
        _LOGGER.exception(f"Ingesting document with ID '{document.import_id}' failed")
        return HandleResult(error=traceback.format_exc(), parser_input=parser_input)

    parser_input = parser_input.copy(
        update={
            "document_cdn_object": uploaded_document_result.cdn_object,
            "document_content_type": uploaded_document_result.content_type,
            "document_md5_sum": uploaded_document_result.md5_sum,
        },
    )

    return HandleResult(parser_input=parser_input)
