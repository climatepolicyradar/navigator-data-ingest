"""A simple API client for creating documents & associations."""
import hashlib
import logging
import os
from functools import lru_cache

import requests
from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    MULTI_FILE_CONTENT_TYPES,
    SUPPORTED_CONTENT_TYPES,
    DocumentUploadResult,
    UnsupportedContentTypeError,
)

API_HOST_ENVVAR = "API_HOST"
MACHINE_USER_EMAIL_ENVVAR = "MACHINE_USER_EMAIL"
MACHINE_USER_PASSWORD_ENVVAR = "MACHINE_USER_PASSWORD"
CDN_PREFIX = "https://cdn.climatepolicyradar.org"

_LOGGER = logging.getLogger(__file__)

META_KEY = "metadata"


@lru_cache()
def _get_api_host():
    """Returns API host configured in environment."""
    return os.environ[API_HOST_ENVVAR].rstrip("/")


@lru_cache()
def _get_machine_user_token():
    username = os.environ[MACHINE_USER_EMAIL_ENVVAR]
    password = os.environ[MACHINE_USER_PASSWORD_ENVVAR]
    api_host = _get_api_host()

    login_data = {
        "username": username,
        "password": password,
    }
    get_token_response = requests.post(f"{api_host}/api/tokens", data=login_data)
    tokens = get_token_response.json()
    access_token = tokens["access_token"]

    return access_token


def upload_document(
    session: requests.Session,
    source_url: str,
    file_name_without_suffix: str,
    document_bucket: str,
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
    upload_result = DocumentUploadResult(
        cloud_url=None,
        md5_sum=None,
        content_type=None,
    )

    try:
        download_response = session.get(source_url, allow_redirects=True, timeout=5)
        if download_response.status_code >= 300:
            _LOGGER.error(
                f"Downloading source document failed: {download_response.status_code} "
                f"{download_response.text}"
            )
            return upload_result

        content_type = download_response.headers["Content-Type"].split(";")[0]
        # Update the result object with the detected content type
        upload_result.content_type = content_type

        # Decide what to do next based on content type
        if content_type in MULTI_FILE_CONTENT_TYPES:
            raise UnsupportedContentTypeError(content_type)

        if content_type not in SUPPORTED_CONTENT_TYPES:
            raise UnsupportedContentTypeError(content_type)

        # Calculate the m5sum & update the result object with the calculated value
        file_content = download_response.content
        file_content_hash = hashlib.md5(file_content).hexdigest()
        upload_result.md5_sum = file_content_hash

        # s3 can only handle paths of up to 1024 bytes. To ensure we don't exceed that,
        # we trim the filename if it's too long
        file_suffix = content_type.split("/")[1]
        filename_max_len = (
            1024
            - len(file_name_without_suffix)
            - len(file_suffix)
            - len(file_content_hash)
            - len("_.")  # length of additional characters for joining path components
        )
        file_name_without_suffix_trimmed = file_name_without_suffix[:filename_max_len]
        file_name = (
            f"{file_name_without_suffix_trimmed}_{file_content_hash}.{file_suffix}"
        )

        _LOGGER.info(
            f"Uploading supported single file document content from '{source_url}' "
            f"to CDN s3 bucket with filename '{file_name}'"
        )
        cdn_url = _store_cached_document(document_bucket, file_name, file_content)
        upload_result.cloud_url = cdn_url

    except UnsupportedContentTypeError as e:
        _LOGGER.warn(
            f"Uploads for document at '{source_url}' could not be completed because "
            f"the content type '{e.content_type}' is not currently supported."
        )
    finally:
        # Always return an upload result, even if it's incomplete
        # TODO: perhaps use the existence of an incomplete output in the future
        return upload_result


def _store_cached_document(
    bucket: str,
    name: str,
    data: bytes,
) -> str:
    clean_name = name.lstrip("/")
    output_file_location = S3Path(f"s3://{bucket}/{clean_name}")
    with output_file_location.open("wb") as output_file:
        output_file.write(data)
    return f"{CDN_PREFIX}/{clean_name}"
