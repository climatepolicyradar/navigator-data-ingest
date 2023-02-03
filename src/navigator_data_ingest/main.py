import logging.config
import os
from concurrent.futures import ProcessPoolExecutor
from typing import cast

import click
import json_logging
from cloudpathlib import CloudPath, S3Path

from navigator_data_ingest.base.api_client import (
    API_HOST_ENVVAR,
    MACHINE_USER_EMAIL_ENVVAR,
    MACHINE_USER_PASSWORD_ENVVAR,
    write_error_file,
    write_parser_input,
)
from navigator_data_ingest.base.new_document_actions import (
    LawPolicyGenerator,
    handle_all_documents,
)
from navigator_data_ingest.base.types import Document, UpdateConfig
from navigator_data_ingest.base.updated_document_actions import (
    handle_document_updates,
    LawPolicyUpdatesGenerator,
)
from navigator_data_ingest.base.utils import get_input_data

REQUIRED_ENV_VARS = [
    API_HOST_ENVVAR,
    MACHINE_USER_EMAIL_ENVVAR,
    MACHINE_USER_PASSWORD_ENVVAR,
]
ENV_VAR_MISSING_ERROR = 10


# Clear existing log handlers so we always log in structured JSON
root_logger = logging.getLogger()
if root_logger.handlers:
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

for _, logger in logging.root.manager.loggerDict.items():
    if isinstance(logger, logging.Logger):
        logger.propagate = True
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {},
    "root": {
        "handlers": ["default"],
        "level": LOG_LEVEL,
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)
json_logging.init_non_web(enable_json=True)
_LOGGER = logging.getLogger(__name__)


@click.command()
@click.option(
    "--pipeline-bucket",
    required=True,
    help="S3 bucket name from which to read/write input/output files",
)
@click.option(
    "--document-bucket",
    required=True,
    help="S3 bucket name in which to store cached documents",
)
@click.option(
    "--input-file",
    required=True,
    help="Location of JSON Document array input file",
)
@click.option(
    "--output-prefix",
    required=True,
    help="Prefix to apply to output files",
)
@click.option(
    "--worker-count",
    required=False,
    default=4,
    help="Number of workers downloading/uploading cached documents",
)
def main(
    pipeline_bucket: str,
    document_bucket: str,
    input_file: str,
    output_prefix: str,
    worker_count: int,
):
    """
    Load documents from source JSON array file, updating details via API.

    :param Optional[str] input_bucket: S3 bucket name from which to read the input file
    :param str input_file: Location of JSON Document array input file
    :param Optional[str] document_bucket: S3 bucket to which to upload documents
    :param str
    :return None:
    """
    if os.getenv("ENV") != "production":
        # for running locally (outside docker)
        from dotenv import load_dotenv

        load_dotenv("../../.env")
        load_dotenv("../../.env.local")
    _check_required_env_vars()

    # Set up input/output paths
    pipeline_bucket_path = S3Path(f"s3://{pipeline_bucket.strip().rstrip('/')}")
    input_file_path = cast(
        S3Path,
        pipeline_bucket_path / f"{input_file.strip().lstrip('/')}",
    )
    output_location_path = cast(
        S3Path,
        pipeline_bucket_path / f"{output_prefix.strip().lstrip('/')}",
    )

    _LOGGER.info(f"Loading Law/Policy document data from '{input_file_path}'")

    input_data = get_input_data(input_file_path)
    document_generator = LawPolicyGenerator(input_data.new_documents)
    document_updates_generator = LawPolicyUpdatesGenerator(input_data.updated_documents)

    errors = []
    # TODO: configure worker count
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        documents_to_process = [
            document
            for document in document_generator.process_source()
            if not _parser_input_already_exists(output_location_path, document)
        ]

        for handle_result in handle_all_documents(
            executor,
            documents_to_process,
            document_bucket,
        ):
            if handle_result.error is not None:
                errors.append(
                    f"ERROR ingesting '{handle_result.parser_input.document_id}': "
                    f"{handle_result.error}"
                )
            _LOGGER.info(
                f"Writing parser input for '{handle_result.parser_input.document_id}"
            )
            write_parser_input(output_location_path, handle_result.parser_input)

        documents_to_update = list(document_updates_generator.update_source())
        # TODO pull from config or env or click argument
        update_config = UpdateConfig(
            pipeline_bucket=pipeline_bucket,
            input_prefix="input",
            parser_input_prefix="parser_input",
            embeddings_input_prefix="embeddings_input",
            indexer_input_prefix="indexer_input",
            archive_prefix="archive",
        )

        for handle_result in handle_document_updates(
            executor,
            documents_to_update,
            update_config,
        ):
            if handle_result.error is not None:
                errors.append(
                    f"ERROR updating '{handle_result.document_update.id}': "
                    f"{handle_result.error}"
                )
                # TODO do something with error and update

    if errors:
        error_output_location_path = cast(
            S3Path,
            pipeline_bucket_path / f"{input_file.strip().lstrip('/')}_errors",
        )
        write_error_file(error_output_location_path, errors)


def _parser_input_already_exists(
    output_location: CloudPath,
    document: Document,
) -> bool:
    output_file_location = cast(
        S3Path,
        output_location / f"{document.import_id}.json",
    )
    if output_file_location.exists():
        _LOGGER.info(
            f"Parser input for document ID '{document.import_id}' already exists"
        )
        return True
    return False


def _check_required_env_vars() -> None:
    fail = False
    for e in REQUIRED_ENV_VARS:
        if e not in os.environ:
            _LOGGER.error(f"Missing environment variable: {e}")
            fail = True

    if fail:
        exit(ENV_VAR_MISSING_ERROR)


if __name__ == "__main__":
    main()
