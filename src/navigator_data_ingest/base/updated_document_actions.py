import logging
import traceback
from concurrent.futures import as_completed, Executor
from datetime import datetime
from typing import Generator, Iterable

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    DocumentUpdate,
    HandleUploadResult,
    UpdateConfig,
    UpdateResult,
)

_LOGGER = logging.getLogger(__file__)


def handle_document_updates(
    executor: Executor,
    source: Iterable[dict[str, UpdateResult]],
    update_config: UpdateConfig,
) -> Generator[HandleUploadResult, None, None]:
    """
    Handle documents updates.

    For each document:
      - Iterate through the document updates and perform the relevant action based upon the update type.
    """
    tasks = {
        executor.submit(
            _update_document,
            update,
            update_config,
        ): update
        for update in source
    }

    for future in as_completed(tasks):
        # check result, handle errors & shut down
        update = tasks[future]
        try:
            handle_result = future.result()
        except Exception:
            _LOGGER.exception(
                f"Updating document '{update.id}' generated an " "unexpected exception."
            )
        else:
            yield handle_result

    _LOGGER.info("Done uploading documents")


def _update_document(
    update: DocumentUpdate,
    update_config: UpdateConfig,
) -> HandleUploadResult:
    """Perform the document update."""
    try:
        action = identify_action(update.updates)
        action(update.id, update_config)
        return HandleUploadResult(document_update=update)

    except Exception:
        _LOGGER.exception(f"Ingesting document with ID '{update.id}' failed")
        return HandleUploadResult(error=traceback.format_exc(), document_update=update)


def _archive_document(
    document_id: str,
    update_config: UpdateConfig,
    file_suffixes=None,
) -> None:
    """Archive the document by copying all instances of the document to the archive s3 directory with timestamp."""
    if file_suffixes is None:
        file_suffixes = ["json", "npy"]
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for prefix in update_config.pipeline_stage_prefixes:
        for suffix in file_suffixes:
            document_path = S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix[1]}/{document_id}.{suffix}"
            )
            archive_path = S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix[1]}/{document_id}/{timestamp}.{suffix}"
            )
            if document_path.exists():
                _LOGGER.info(
                    "Archiving document %s from %s to %s",
                    document_id,
                    document_path,
                    archive_path,
                )
                document_path.rename(archive_path)


def update_dont_parse(
    document_id: str,
    update_config: UpdateConfig,
) -> None:
    pass


def update_parse(
    document_id: str,
    update_config: UpdateConfig,
) -> None:
    pass


def identify_action(updates: dict) -> callable:
    """Identify the action to be performed based upon the update type."""
    # TODO build out taxonomy of actions
    return _archive_document
