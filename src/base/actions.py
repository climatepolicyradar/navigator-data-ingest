import logging
from concurrent.futures import as_completed, Executor
from typing import Generator, Sequence

import requests
from slugify import slugify

from base.types import (
    DEFAULT_DESCRIPTION,
    SINGLE_FILE_CONTENT_TYPES,
    Document,
    DocumentGenerator,
    DocumentParserInput,
    DocumentRelationship,
    DocumentUploadResult,
)
from base.api_client import (
    upload_document,
    post_document,
    post_relationship,
    put_document_relationship,
)

_LOGGER = logging.getLogger(__file__)


def handle_all_documents(
    executor: Executor, source: DocumentGenerator
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
        executor.submit(_handle_documents, document_group): document_group
        for document_group in source.process_source()
    }

    for future in as_completed(tasks):
        # check result, handle errors & shut down
        document_group: Sequence[Document] = tasks[future]
        try:
            result = future.result()
        except Exception:
            _LOGGER.exception(
                "Handling the following document group generated an exception: "
                f"{document_group}"
            )
        else:
            _LOGGER.debug(f"Raw result: {result}")
            _LOGGER.info(
                "Created documents & required relationships for "
                f"'{[doc.import_id for doc in document_group]}'"
            )
            for doc in document_group:
                if doc.url is not None and doc.content_type is not None:
                    url_for_parser = doc.source_url
                    if doc.content_type in SINGLE_FILE_CONTENT_TYPES:
                        url_for_parser = doc.url
                    yield DocumentParserInput(
                        url=url_for_parser,
                        import_id=doc.import_id,
                        content_type=doc.content_type,
                        document_slug="",  # TODO: implement
                    )

    _LOGGER.info("Done uploading documents")


def _upload_document(
    session: requests.Session,
    document: Document,
) -> DocumentUploadResult:
    """
    Upload a single document.

    :param requests.Session session: The session to use for HTTP requests
    :param Document document: The document description
    :return DocumentUploadResult: Details of the document content & upload location
    """
    # Replace forward slashes because S3 recognises them as pseudo-directory separators
    doc_slug = slugify(document.name)
    doc_geo = document.geography
    doc_year = document.publication_ts.year
    file_name = f"{doc_geo}_{doc_year}_{doc_slug}"

    return upload_document(
        session,
        document.source_url,
        file_name,
    )


def _handle_documents(documents: Sequence[Document]) -> None:
    """
    Create database entries & upload document source files.

    :param Sequence[Document] documents: A related group of documents to upload.
    """
    session = requests.Session()

    document_ids = []
    for document in documents:
        if not document.source_url.strip():
            # TODO: Make document upload more resilient
            # TODO: Decide on Update/Create Document entries in database
            # TODO: Implement Update Document entries in database
            upload_result = _upload_document(session, document)
            document.url = upload_result.cloud_url
            document.md5sum = upload_result.md5sum

        else:
            _LOGGER.info(
                f"Skipping upload for '{document.source}:{document.import_id}:"
                f"{document.name}' because the source URL is empty"
            )

        try:
            create_document_response = post_document(session=session, document=document)
            if create_document_response.status_code >= 300:
                # TODO: More nuanced status response handling
                _LOGGER.warning(
                    f"Failed to create entry in the database for {document}"
                )
            else:
                document_ids.append(create_document_response.json()["id"])
        except Exception:
            _LOGGER.exception(
                f"Uploading document with URL {document.source_url} failed"
            )

    if len(documents) > 1:
        relationship = DocumentRelationship(
            name="Related",
            description=f"Related Documents {DEFAULT_DESCRIPTION}",
            type="Document Group",
        )

        create_relationship_response = post_relationship(
            session=session,
            relationship=relationship,
        )
        if create_relationship_response.status_code >= 300:
            # TODO: More nuanced status response handling
            _LOGGER.warning(
                f"Failed to create entry in the database for {relationship}"
            )
        else:
            for document_id in document_ids:
                relationship_id = create_relationship_response.json()["id"]
                create_doc_relationship_link_response = put_document_relationship(
                    session=session,
                    relationship_id=relationship_id,
                    document_id=document_id,
                )
                if create_doc_relationship_link_response.status_code >= 300:
                    # TODO: More nuanced status response handling
                    _LOGGER.warning(
                        f"Failed to create link between document id '{document_id}' "
                        f"and relationship with id '{relationship_id}"
                    )
