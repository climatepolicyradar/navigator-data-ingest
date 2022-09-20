"""A simple API client for creating documents & associations."""
import hashlib
import logging
import os
from functools import lru_cache

import requests

from src.base.types import (
    SUPPORTED_CONTENT_TYPES,
    ADDITIONAL_SUPPORTED_CONTENT_TYPES,
    Document,
    DocumentRelationship,
    DocumentUploadResult,
)

_LOGGER = logging.getLogger(__file__)


def _get_api_host():
    """Returns API host configured in environment."""
    return os.getenv("API_HOST", "http://backend:8888").rstrip("/")


@lru_cache()
def _get_machine_user_token():
    username = os.getenv("MACHINE_USER_LOADER_EMAIL")
    password = os.getenv("MACHINE_USER_LOADER_PASSWORD")
    api_host = _get_api_host()

    login_data = {
        "username": username,
        "password": password,
    }
    r = requests.post(f"{api_host}/api/tokens", data=login_data)
    tokens = r.json()
    a_token = tokens["access_token"]

    return a_token


def post_document(session: requests.Session, document: Document) -> requests.Response:
    """
    Make a request to the documents endpoint to create an entry for the given document.

    :param Document document: Details of the document to add to the database
    :return requests.Response: The response from the backend server
    """
    machine_user_token = _get_machine_user_token()
    api_host = _get_api_host()

    headers = {
        "Authorization": "Bearer {}".format(machine_user_token),
        "Accept": "application/json",
    }
    response = session.post(
        f"{api_host}/api/v1/documents",
        headers=headers,
        json=document.dict(),
    )
    return response


def post_relationship(
    session: requests.Session, relationship: DocumentRelationship
) -> requests.Response:
    """
    Make a request to the document relationships endpoint to create a new relationship.

    :param DocumentRelationship relationship: The relationship to add to the database
    :return requests.Response: The response from the backend server
    """
    machine_user_token = _get_machine_user_token()
    api_host = _get_api_host()

    headers = {
        "Authorization": "Bearer {}".format(machine_user_token),
        "Accept": "application/json",
    }
    response = session.post(
        f"{api_host}/api/v1/document-relationships",
        headers=headers,
        json=relationship.dict(),
    )
    return response


def put_document_relationship(
    session: requests.Session,
    relationship_id: str,
    document_id: str,
) -> requests.Response:
    """
    Make a request to create a new link between a document and a relationship.

    :param str relationship_id: The ID of the relationship to which to add a document
    :param str document_id: The ID of the document to link to a relationship
    :return requests.Response: The response from the backend server
    """
    machine_user_token = _get_machine_user_token()
    api_host = _get_api_host()

    headers = {
        "Authorization": "Bearer {}".format(machine_user_token),
        "Accept": "application/json",
    }
    response = session.put(
        (
            f"{api_host}/api/v1/document-relationships/"
            f"{relationship_id}/documents/{document_id}"
        ),
        headers=headers,
    )
    return response


def upload_document(
    session: requests.Session, source_url: str, file_name_without_suffix: str
) -> DocumentUploadResult:
    """
    Upload a document to the cloud, and returns the cloud URL.

    The remote document will have the specified file_name_without_suffix_{md5_hash},
    where md5_hash is the hash of the file and the suffix is determined from the content type.
    `file_name_without_suffix` will be trimmed if the total path length exceeds 1024 bytes,
    which is the S3 maximum path length.

    TODO stream the download/upload instead of downloading all-at-once first.

    :return str: the remote URL and the md5_sum of its contents
    """
    # download the document
    download_response = session.get(source_url, allow_redirects=True)
    content_type = download_response.headers["Content-Type"].split(";")[0]

    # TODO: in the event of HTML, handle appropriately
    if content_type in ADDITIONAL_SUPPORTED_CONTENT_TYPES:
        _LOGGER.warn(
            "Uploads for complex document structures are not currently fully supported"
        )

    if content_type not in SUPPORTED_CONTENT_TYPES:
        raise Exception(f"Unsupported content type: {content_type}")

    _LOGGER.debug(f"Uploading document at {source_url}")

    file_content = download_response.content
    file_content_hash = hashlib.md5(file_content).hexdigest()

    # determine the remote file name, including folder structure
    parts = file_name_without_suffix.split("-")
    folder_path = parts[0] + "/" + parts[1] + "/"
    file_suffix = content_type.split("/")[1]

    # s3 can only handle paths of up to 1024 bytes. To ensure we don't exceed that,
    # we trim the filename if it's too long
    filename_max_len = (
        1024 - len(folder_path) - len(file_suffix) - len(file_content_hash) - len("_.")
    )
    file_name_without_suffix_trimmed = file_name_without_suffix[:filename_max_len]
    file_name = f"{file_name_without_suffix_trimmed}_{file_content_hash}.{file_suffix}"

    # puts docs in folder <country_code>/<publication_year>/<file_name>
    full_path = parts[0] + "/" + parts[1] + "/" + file_name

    _LOGGER.info(f"Uploading {source_url} content to {full_path}")

    machine_user_token = _get_machine_user_token()
    api_host = _get_api_host()

    headers = {
        "Authorization": "Bearer {}".format(machine_user_token),
        "Accept": "application/json",
    }
    _LOGGER.info(
        f"Making POST request to: '{api_host}/api/v1/document' for {full_path}"
    )
    response = session.post(
        f"{api_host}/api/v1/document",
        headers=headers,
        files={"file": (full_path, file_content, content_type)},
    )
    response_json = response.json()

    if "url" in response_json:
        # For single file content types, return the URL to the CPR cache copy
        return DocumentUploadResult(
            cloud_url=response_json["url"],
            md5sum=file_content_hash,
            content_type=content_type,
        )

    raise Exception(response_json["detail"])
