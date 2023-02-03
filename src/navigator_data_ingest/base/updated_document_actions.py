import logging
import traceback
from concurrent.futures import as_completed, Executor
from datetime import datetime
from typing import Generator, Iterable

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    DocumentUpdate,
    HandleUploadResult,
    DocumentUpdateGenerator,
    UpdateConfig,
)

_LOGGER = logging.getLogger(__file__)


class LawPolicyUpdatesGenerator(DocumentUpdateGenerator):
    """A generator for updated Document objects for inspection & update/archive"""

    def __init__(self, json_updates: dict):
        self.json_updates = json_updates

    def update_source(self) -> Generator[DocumentUpdate, None, None]:
        """Generate documents for updating in s3 from the configured source."""
        for d in self.json_updates:
            yield DocumentUpdate(id=d, updates=self.json_updates[d])


def handle_document_updates(
    executor: Executor,
    source: Iterable[DocumentUpdate],
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
    update_config: UpdateConfig,
    timestamp: str,
    document_id: str,
    prefix: str,
    file_suffix: str,
) -> None:
    """Archive the document."""
    _LOGGER.info("Archiving document %s", document_id)
    document_path = S3Path(
        f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.{file_suffix}"
    )
    archive_path = S3Path(
        f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}.{file_suffix}"
    )
    _LOGGER.info(
        "Archiving document %s from %s to %s", document_id, document_path, archive_path
    )
    if document_path.exists():
        document_path.rename(archive_path)


# TODO feels like we can just pass in the UpdateConfig directly to the point of use rather than cart through the code
#  This does allow us to get from a click argument though
def _archive_document_s3_instances(
    document_id: str,
    update_config: UpdateConfig,
) -> None:
    """Archive the document by copying all instances of the document to the archive s3 directory with the relevant
    timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    archive_config = {
        update_config.parser_input_prefix: ["json"],
        update_config.embeddings_input_prefix: ["json"],
        update_config.indexer_input_prefix: ["json", "npy"],
    }

    for prefix, suffixes in archive_config.items():
        for suffix in suffixes:
            _archive_document(update_config, timestamp, document_id, prefix, suffix)


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
    return _archive_document_s3_instances
