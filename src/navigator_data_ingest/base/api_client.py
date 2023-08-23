"""A simple API client for creating documents & associations."""
import hashlib
import json
import logging
from typing import cast

import requests
from cloudpathlib import CloudPath, S3Path
from cpr_data_access.parser_models import ParserInput
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential

from navigator_data_ingest.base.types import (
    MULTI_FILE_CONTENT_TYPES,
    SUPPORTED_CONTENT_TYPES,
    FILE_EXTENSION_MAPPING,
    UploadResult,
    UnsupportedContentTypeError,
)

_LOGGER = logging.getLogger(__file__)

META_KEY = "metadata"


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
    """
    # download the document
    _LOGGER.info(f"Downloading document from '{source_url}' for {import_id}")
    upload_result = UploadResult(
        cdn_object=None,
        md5_sum=None,
        content_type=None,
    )

    try:
        download_response = _download_from_source(session, source_url)
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
        file_hash = hashlib.md5(file_content).hexdigest()
        upload_result.md5_sum = file_hash

        # ext4 used in Amazon Linux /tmp directory has a max filename length of
        # 255 bytes, so trim to ensure we don't exceed that. Choose 240 initially to
        # allow for suffix.
        file_name_max_fs_len_no_suffix = file_name_without_suffix[:200]
        while len(file_name_max_fs_len_no_suffix.encode("utf-8")) > 200:
            file_name_max_fs_len_no_suffix = file_name_max_fs_len_no_suffix[:-5]

        # s3 can only handle paths of up to 1024 bytes. To ensure we don't exceed that,
        # we trim the filename if it's too long
        file_suffix = FILE_EXTENSION_MAPPING.get(content_type, "")
        filename_max_len = (
            1024
            - len(s3_prefix)
            - len(file_suffix)
            - len(file_hash)
            - len("_.")  # length of additional characters for joining path components
        )
        file_name_no_suffix_trimmed = file_name_max_fs_len_no_suffix[:filename_max_len]
        # Safe not to loop over the encoding of file_name because everything we're
        # adding is 1byte == 1char
        file_name = (
            f"{s3_prefix}/{file_name_no_suffix_trimmed}_{file_hash}{file_suffix}"
        )

        _LOGGER.info(
            f"Uploading supported single file document content from '{source_url}' "
            f"to CDN s3 bucket with filename '{file_name}'"
        )
        cdn_object = _store_document_in_cache(document_bucket, file_name, file_content)
        upload_result.cdn_object = cdn_object

    except UnsupportedContentTypeError as e:
        _LOGGER.warn(
            f"Uploads for document {import_id} at '{source_url}' could not be completed because "
            f"the content type '{e.content_type}' is not currently supported."
        )
    except Exception:
        _LOGGER.exception(f"Downloading source document {import_id} failed")
    finally:
        # Always return an upload result, even if it's incomplete
        # TODO: perhaps use the existence of an incomplete output in the future
        return upload_result


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
)
def _download_from_source(
    session: requests.Session, source_url: str
) -> requests.Response:
    # Try the orginal source url
    download_response = session.get(source_url, allow_redirects=True, timeout=5)

    # TODO this is a hack and we should handle source urls upstream in the backend
    if download_response.status_code == 404:
        # mutation 1 - remove %
        download_response = session.get(
            source_url.replace("%", ""), allow_redirects=True, timeout=5
        )

    if download_response.status_code == 404:
        # mutation 2 - replace % with the encoded version, i.e. %25
        download_response = session.get(
            source_url.replace("%", "%25"), allow_redirects=True, timeout=5
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
        output_file.write(json.dumps(parser_input.to_json(), indent=2))


@retry(
    stop=stop_after_attempt(4),
    wait=wait_random_exponential(multiplier=1, min=1, max=10),
)
def write_error_file(
    output_location: CloudPath,
    errors: list[str],
) -> None:
    with output_location.open("w") as output_file:
        output_file.write(json.dumps(errors, indent=2))
