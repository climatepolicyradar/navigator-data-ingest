import logging.config
import os
from concurrent.futures import ProcessPoolExecutor
from typing import cast

import click
import json_logging
from cloudpathlib import S3Path

from navigator_data_ingest.base.api_client import (
    write_error_file,
    write_parser_input,
)
from navigator_data_ingest.base.new_document_actions import handle_new_documents
from navigator_data_ingest.base.types import UpdateConfig
from navigator_data_ingest.base.updated_document_actions import handle_document_updates
from navigator_data_ingest.base.utils import LawPolicyGenerator, check_required_env_vars

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
    required=False,
    default="parser_input",
    help="Prefix to apply to output files, this s3 directory is the parser input",
)
@click.option(
    "--embeddings-input-prefix",
    required=False,
    default="embeddings_input",
    help="S3 prefix containing the embeddings input files",
)
@click.option(
    "--indexer-input-prefix",
    required=False,
    default="indexer_input",
    help="S3 prefix containing the indexer input files",
)
@click.option(
    "--archive-prefix",
    required=False,
    default="archive",
    help="S3 prefix to which to archive documents",
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
    embeddings_input_prefix: str,
    indexer_input_prefix: str,
    archive_prefix: str,
    worker_count: int,
):
    """
    Load documents from source JSON array file, updating details via API.

    param pipeline_bucket: S3 bucket name from which to read/write input/output files
    param document_bucket: S3 bucket name in which to store cached documents
    param input_file: Location of JSON Document array input file
    param parser_input_prefix: Prefix to apply to output files that contains the parser input files
    param embeddings_input_prefix: S3 prefix containing the embeddings input files
    param indexer_input_prefix: S3 prefix containing the indexer input files
    param archive_prefix: S3 prefix to which to archive documents
    param worker_count: Number of workers downloading/uploading cached documents
    return: None
    """
    check_required_env_vars()

    pipeline_bucket_path = S3Path(f"s3://{pipeline_bucket.strip().rstrip('/')}")
    input_file_path = cast(
        S3Path,
        pipeline_bucket_path / f"{input_file.strip().lstrip('/')}",
    )
    output_location_path = cast(
        S3Path,
        pipeline_bucket_path / f"{output_prefix.strip().lstrip('/')}",
    )
    _LOGGER.info(
        "Loading and updating Law/Policy document data.",
        extra={
            "props": {
                "input_file": str(input_file_path),
                "output_location": str(output_location_path),
            }
        },
    )

    document_generator = LawPolicyGenerator(input_file_path, output_location_path)
    errors = []

    # TODO: configure worker count
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        update_config = UpdateConfig(
            pipeline_bucket=pipeline_bucket,
            input_prefix=input_file_path.key.replace(input_file_path.name, ""),
            parser_input=output_prefix,
            embeddings_input=embeddings_input_prefix,
            indexer_input=indexer_input_prefix,
            archive_prefix=archive_prefix,
        )

        for handle_result in handle_document_updates(
            executor,
            document_generator.process_updated_documents(),
            update_config,
        ):
            for result in handle_result:
                if str(result.error) != "[]":
                    errors.append(
                        f"ERROR updating '{result.document_id}': {result.error}"
                    )

            _LOGGER.info(
                "Writing ERROR to JSON_ERRORS file",
                extra={
                    "props": {
                        "errors": errors,
                    }
                },
            )

        for handle_result in handle_new_documents(
            executor,
            document_generator.process_new_documents(),
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

    if len(errors) > 0:
        error_output_location_path = cast(
            S3Path,
            pipeline_bucket_path / f"{input_file.strip().lstrip('/')}_errors",
        )
        write_error_file(error_output_location_path, errors)


if __name__ == "__main__":
    main()
