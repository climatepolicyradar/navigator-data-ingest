"""A simple API client for creating documents & associations."""
import hashlib
import logging
import os
from functools import lru_cache

import requests

from base.types import (
    MULTI_FILE_CONTENT_TYPES,
    SUPPORTED_CONTENT_TYPES,
    DocumentUploadResult,
)

_LOGGER = logging.getLogger(__file__)

META_KEY = "metadata"


def _get_api_host():
    """Returns API host configured in environment."""
    return os.getenv("API_HOST", "http://localhost:8888").rstrip("/")


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


def upload_document(
    session: requests.Session, source_url: str, file_name_without_suffix: str
) -> DocumentUploadResult:
    """
    Upload a document to the cloud, and returns the cloud URL.

    The remote document will have the specified file_name_without_suffix_{md5_hash},
    where md5_hash is the hash of the file and the suffix is determined from the content type.
    `file_name_without_suffix` will be trimmed if the total path length exceeds 1024 bytes,
    which is the S3 maximum path length.

    :param requests.Session session: The session used for making the request.
    :return DocumentUploadResult: the remote URL and the md5_sum of its contents
    """
    # download the document
    _LOGGER.info(f"Downloading document from '{source_url}'")

    download_response = session.get(source_url, allow_redirects=True, timeout=5)
    if download_response.status_code >= 300:
        _LOGGER.error(f"Could not download source document for '{source_url}'")
        # TODO: Proper exception
        raise Exception(f"Upload failed {download_response.text}")

    content_type = download_response.headers["Content-Type"].split(";")[0]
    if content_type in MULTI_FILE_CONTENT_TYPES:
        _LOGGER.warn(
            "Uploads for complex document structures are not currently fully supported"
        )
        return DocumentUploadResult(
            cloud_url=None,
            md5_sum=None,
            content_type=content_type,
        )

    if content_type not in SUPPORTED_CONTENT_TYPES:
        _LOGGER.warn(f"Unsupported content type: {content_type}")
        return DocumentUploadResult(
            cloud_url=None,
            md5_sum=None,
            content_type=content_type,
        )

    _LOGGER.info(f"Uploading supported single file document at '{source_url}'")

    file_content = download_response.content
    file_content_hash = hashlib.md5(file_content).hexdigest()
    file_suffix = content_type.split("/")[1]

    # s3 can only handle paths of up to 1024 bytes. To ensure we don't exceed that,
    # we trim the filename if it's too long
    filename_max_len = (
        1024
        - len(file_name_without_suffix)
        - len(file_suffix)
        - len(file_content_hash)
        - len("_.")  # length of additional characters for joining path components
    )
    file_name_without_suffix_trimmed = file_name_without_suffix[:filename_max_len]
    file_name = f"{file_name_without_suffix_trimmed}_{file_content_hash}.{file_suffix}"

    _LOGGER.info(f"Uploading {source_url} content to {file_name}")

    machine_user_token = _get_machine_user_token()
    api_host = _get_api_host()

    headers = {
        "Authorization": "Bearer {}".format(machine_user_token),
        "Accept": "application/json",
    }
    _LOGGER.info(
        f"Making POST request to: '{api_host}/api/v1/document' for {file_name}"
    )
    create_upload_url_response = session.post(
        url=f"{api_host}/api/v1/document-uploads",
        headers=headers,
        json={
            "filename": file_name,
            "overwrite": False,
        },
    )
    if create_upload_url_response.status_code >= 300:
        _LOGGER.error(f"Failed to create upload URL for {file_name}")
        raise Exception(
            f"Failed to create upload URL: {create_upload_url_response.text}"
        )

    if create_upload_url_response.status_code == 201:
        create_upload_url_response_json = create_upload_url_response.json()
        _LOGGER.info(
            f"Uploading to: {create_upload_url_response_json['presigned_upload_url']}"
        )
        upload_response = requests.put(
            create_upload_url_response_json["presigned_upload_url"],
            data=file_content,
            headers={"Content-Type": content_type},
        )
    else:
        _LOGGER.error(f"Unexpected response when creating upload URL for {file_name}")
        raise Exception(
            "Unexpected response when creating upload URL: "
            f"{create_upload_url_response.status_code} "
            f"{create_upload_url_response.text}"
        )

    if upload_response.status_code >= 300:
        _LOGGER.error(f"Failed to upload content for {file_name}")
        raise Exception(f"Failed to upload content: {create_upload_url_response.text}")

    return DocumentUploadResult(
        cloud_url=create_upload_url_response_json["cdn_url"],
        md5_sum=file_content_hash,
        content_type=content_type,
    )
