import json
import logging
import traceback
from concurrent.futures import as_completed, Executor
from datetime import datetime
from typing import Generator, List, Union, Callable

from cloudpathlib import S3Path
from pydantic import BaseModel

from navigator_data_ingest.base.types import (
    UpdateConfig,
    UpdateResult,
    UpdateDocumentResult,
    UpdateFields,
    DocumentStatusTypes,
    UpdateTypes,
)

_LOGGER = logging.getLogger(__file__)


def handle_document_updates(
    executor: Executor,
    source: Generator[dict[str, List[UpdateResult]]],
    update_config: UpdateConfig,
) -> List[UpdateDocumentResult, None, None]:
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
                "Updating document generated an unexpected exception.",
                extra={
                    "props": {
                        "update": update,
                    },
                },
            )
        else:
            yield handle_result

    _LOGGER.info("Done uploading documents")


class Action(BaseModel):
    """Base class for actions."""

    update: UpdateResult
    action: Callable


def _update_document(
    doc_updates: dict[str, List[UpdateResult]],
    update_config: UpdateConfig,
) -> List[UpdateDocumentResult]:
    """Perform the document update."""
    doc_id = list(doc_updates.keys())[0]

    actions = [
        Action(action=identify_action(update), update=update)
        for update in doc_updates[doc_id]
    ]

    return [
        UpdateDocumentResult(
            error=action.action({doc_id: action.update}, update_config),
            document_id=doc_id,
            update=action.update,
        )
        for action in order_actions(actions)
    ]


def order_actions(actions: List[Action]) -> List[callable]:
    """
    Order the update actions to be performed on an s3 document based upon the action type.

    We need to ensure that we make object updates before archiving a document.
    """
    ordering = [publish.__name__, update_dont_parse.__name__, archive.__name__]
    priorities = {letter: index for index, letter in enumerate(ordering)}

    return [
        action
        for action in sorted(
            actions, key=lambda action: priorities[action.callable.__name__]
        )
    ]


def identify_action(update: UpdateResult) -> callable:
    """Identify the action to be performed based upon the update type and field."""
    if (
        update.field == UpdateFields.SOURCE_URL.name
        and update.type == UpdateTypes.PHYSICAL_DOCUMENT.name
    ) or (
        update.field == UpdateFields.DOCUMENT_STATUS.name
        and update.type == UpdateTypes.PHYSICAL_DOCUMENT.name
        and update.csv_value == DocumentStatusTypes.DELETED.name
    ):
        return archive

    elif (
        update.field == UpdateFields.NAME.name
        and update.type == UpdateTypes.FAMILY.name
    ) or (
        update.field == UpdateFields.DESCRIPTION.name
        and update.type == UpdateTypes.FAMILY.name
    ):
        return update_dont_parse

    elif (
        update.field == UpdateFields.DOCUMENT_STATUS.name
        and update.type == UpdateTypes.PHYSICAL_DOCUMENT.name
        and update.csv_value == DocumentStatusTypes.PUBLISHED.name
    ):
        return publish

    else:
        raise NotImplementedError(
            f"Update type {update.type} with field {update.field} is not implemented."
        )


def perform_archive(document_path, archive_path) -> Union[str, None]:
    """Rename the document to the archive path."""
    try:
        if document_path.exists():
            document_path.rename(archive_path)
            _LOGGER.info(
                "Document archived.",
                extra={
                    "props": {
                        "document_path": document_path,
                        "archive_path": archive_path,
                    }
                },
            )
    except Exception as e:
        _LOGGER.exception(
            "Archiving document failed.",
            extra={
                "props": {
                    "document_path": document_path,
                    "archive_path": archive_path,
                    "error": e,
                }
            },
        )
        return traceback.format_exc()
    return None


def archive(
    update: dict[str, UpdateResult],
    update_config: UpdateConfig,
) -> List[str]:
    """Archive the document by copying all instances of the document to the archive s3 directory with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    document_id = list(update.keys())[0]
    errors = [
        perform_archive(
            document_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.json"
            ),
            archive_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}.json"
            ),
        )
        for prefix in [
            update_config.parser_input,
            update_config.embeddings_input,
            update_config.indexer_input,
        ]
    ]
    errors.append(
        perform_archive(
            document_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.indexer_input}/{document_id}.npy"
            ),
            archive_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{update_config.indexer_input}/{document_id}/{timestamp}.npy"
            ),
        )
    )
    return errors


def publish(
    update: dict[str, UpdateResult],
    update_config: UpdateConfig,
) -> Union[str, None]:
    """Publish a deleted/archived document by copying all instances of the document to the live s3 directories."""
    _LOGGER.info("Publishing document %s, %s", update, update_config)
    return None


def update_file_field(
    document_path: S3Path,
    field: str,
    new_value: Union[str, datetime],
    existing_value: Union[str, datetime],
) -> Union[str, None]:
    """Update the value of a field in a json object within s3 with the new value."""
    if document_path.exists():
        _LOGGER.info(
            "Updating document field.",
            extra={
                "props": {
                    "document_path": document_path,
                    "field": field,
                    "value": new_value,
                    "existing_value": existing_value,
                }
            },
        )
        document = json.loads(document_path.read_text())
        try:
            assert document[field] == existing_value
        except AssertionError:
            _LOGGER.error(
                "Field value mismatch - expected value not found in s3 object.",
                extra={
                    "props": {
                        "document_path": document_path,
                        "field": field,
                        "value": new_value,
                        "existing_value": existing_value,
                        "document": document,
                    }
                },
            )
            return traceback.format_exc()
        except KeyError:
            _LOGGER.error(
                "Field not found in s3 object.",
                extra={
                    "props": {
                        "document_path": document_path,
                        "field": field,
                        "value": new_value,
                        "existing_value": existing_value,
                        "document": document,
                    }
                },
            )
            return traceback.format_exc()
        document[field] = new_value
        document_path.write_text(json.dumps(document))
        return None
    else:
        _LOGGER.error(
            "Expected to update document but it doesn't exist.",
            extra={
                "props": {
                    "document_path": document_path,
                }
            },
        )
        return "NotFoundError: Expected to update document but it doesn't exist."


def update_dont_parse(
    update: dict[str, UpdateResult],
    update_config: UpdateConfig,
) -> List[str]:
    """
    Update the json objects and remove the npy file in the s3 pipeline cache.

    This is done so that the npy file of embeddings is recreated to reflect the change in the json object field and
    incorporated into the corpus during the next pipeline run whilst not triggering re-parsing of the document.
    """
    document_id = list(update.keys())[0]
    update_ = update[document_id]
    errors = [
        update_file_field(
            document_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.json"
            ),
            field=update_.field,
            new_value=update_.csv_value,
            existing_value=update_.db_value,
        )
        for prefix in [
            update_config.parser_input,
            update_config.embeddings_input,
            update_config.indexer_input,
        ]
    ]

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    errors.append(
        perform_archive(
            document_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.embeddings_input}/{document_id}.npy"
            ),
            archive_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{update_config.embeddings_input}/{document_id}/{timestamp}.npy"
            ),
        )
    )
    return errors
