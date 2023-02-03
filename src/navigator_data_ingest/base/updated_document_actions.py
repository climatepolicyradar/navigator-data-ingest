import logging
import traceback
from concurrent.futures import as_completed, Executor
from typing import Generator, Iterable

from navigator_data_ingest.base.types import (
    DocumentUpdate,
    HandleUploadResult,
    DocumentUpdateGenerator,
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
        document_bucket: str,
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
            document_bucket,
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
                f"Updating document '{update.id}' generated an "
                "unexpected exception."
            )
        else:
            yield handle_result

    _LOGGER.info("Done uploading documents")


def _update_document(
        update: DocumentUpdate,
        document_bucket: str,
) -> HandleUploadResult:
    """Perform the document update."""
    try:
        action = identify_action(update.updates)
        action(update.id)

    except Exception:
        _LOGGER.exception(f"Ingesting document with ID '{update.id}' failed")
        return HandleUploadResult(error=traceback.format_exc(), document_update=update)


def _archive_document(
        document_id: str,
) -> None:
    """Archive the document by copying all instances of the document to the archive s3 directory with the relevant
    timestamp. """
    _LOGGER.info("Archiving document %s", document_id)


def identify_action(updates: dict) -> callable:
    """Identify the action to be performed based upon the update type."""
    # TODO build out taxonomy of actions
    return _archive_document
