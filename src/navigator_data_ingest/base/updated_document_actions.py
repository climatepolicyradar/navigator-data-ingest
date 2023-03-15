import json
import logging
import traceback
from concurrent.futures import as_completed, Executor
from datetime import datetime
from typing import Generator, List, Union, Callable, Tuple

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    UpdateConfig,
    Update,
    UpdateResult,
    DocumentStatusTypes,
    UpdateTypes,
    Action,
)

_LOGGER = logging.getLogger(__file__)


def handle_document_updates(
    executor: Executor,
    source: Generator[Tuple[str, List[Update]], None, None],
    update_config: UpdateConfig,
) -> Generator[List[UpdateResult], None, None]:
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
            # TODO find an identifier for the document
            _LOGGER.exception(
                "Updating document generated an unexpected exception.",
                extra={"props": {"document_id": str(update[0])}},
            )
        else:
            yield handle_result

    _LOGGER.info("Done updating documents.")


def _update_document(
    doc_updates: Tuple[str, List[Update]],
    update_config: UpdateConfig,
) -> List[UpdateResult]:
    """Perform the document update."""
    document_id, updates = doc_updates
    _LOGGER.info("Updating document.", extra={"props": {"document_id": document_id}})

    actions = [
        Action(action=identify_action(update), update=update) for update in updates
    ]
    _LOGGER.info(
        "Identified actions for document.",
        extra={
            "props": {
                "document_id": document_id,
                "actions": str([action.action.__name__ for action in actions]),
            }
        },
    )

    return [
        UpdateResult(
            error=str(action.action((document_id, action.update), update_config)),
            document_id=document_id,
            update=action.update,
        )
        for action in order_actions(actions)
    ]


def identify_action(update: Update) -> Callable:
    """Identify the action to be performed based upon the update type and field."""
    _LOGGER.info(
        "Identifying action with the following config.",
        extra={
            "props": {
                "update_type": update.type,
                "update_csv_value": update.csv_value,
                "update_db_value": update.db_value,
            },
        },
    )

    if update.type in [UpdateTypes.SOURCE_URL.value]:
        return parse

    elif (
        update.type == UpdateTypes.DOCUMENT_STATUS.value
        and update.csv_value == DocumentStatusTypes.DELETED.value
    ):
        return archive

    elif (
        update.type == UpdateTypes.DOCUMENT_STATUS.value
        and update.csv_value == DocumentStatusTypes.PUBLISHED.value
    ):
        return publish

    elif update.type in [UpdateTypes.NAME.value, UpdateTypes.DESCRIPTION.value]:
        return update_dont_parse

    else:
        raise ValueError(
            f"Update type {update.type} is not supported. "
            f"Supported update types are: {[name.value for name in UpdateTypes]}"
        )


def order_actions(actions: List[Action]) -> List[Action]:
    """
    Order the update actions to be performed on an s3 document based upon the action type.

    We need to ensure that we make object updates before archiving a document.
    """
    ordering = [
        publish.__name__,
        update_dont_parse.__name__,
        parse.__name__,
        archive.__name__,
    ]
    priorities = {letter: index for index, letter in enumerate(ordering)}
    _LOGGER.info(
        "Ordering actions.",
        extra={"props": {"priorities": str(priorities)}},
    )

    ordered_actions = [
        action
        for action in sorted(
            actions, key=lambda action: priorities[action.action.__name__]
        )
    ]
    _LOGGER.info(
        "Actions ordered.",
        extra={
            "props": {
                "actions_initial": str([action.action.__name__ for action in actions]),
                "actions_final": str(
                    [action.action.__name__ for action in ordered_actions]
                ),
            }
        },
    )

    return ordered_actions


def archive(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[str]:
    """Archive the document by copying all instances of the document to the archive s3 directory with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    document_id, document_update = update
    _LOGGER.info(
        "Archiving all instances of document.",
        extra={
            "props": {
                "document_id": document_id,
                "timestamp": timestamp,
            }
        },
    )

    errors = [
        rename(
            existing_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.{suffix}"
            ),
            rename_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}.{suffix}"
            ),
        )
        for prefix, suffix in [
            (update_config.parser_input, "json"),
            (update_config.embeddings_input, "json"),
            (update_config.indexer_input, "json"),
            (update_config.indexer_input, "npy"),
        ]
    ]
    return errors


def get_latest_timestamp(
    document_id: str, update_config: UpdateConfig
) -> Union[datetime, None]:
    """Get the latest time stamp for the archived instances of a document in the archive s3 directory."""
    document_archived_files = [
        S3Path(
            f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/"
        ).glob("*")
        for prefix in [
            update_config.parser_input,
            update_config.embeddings_input,
            update_config.indexer_input,
            update_config.indexer_input,
        ]
    ]

    if document_archived_files == []:
        return None

    archive_timestamps = [
        datetime.strptime(file.name.split(".")[0], "%Y-%m-%d-%H-%M-%S")
        for files in document_archived_files
        for file in files
    ]

    if archive_timestamps == []:
        return None

    return max(archive_timestamps)


def publish(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[str]:
    """Publish a deleted/archived document by copying all instances of the document to the live s3 directories."""
    document_id, document_update = update
    _LOGGER.info("Publishing document.", extra={"props": {"doc_id": document_id}})
    timestamp = get_latest_timestamp(document_id, update_config)

    if timestamp is None:
        _LOGGER.info(
            "Document has no archived files to publish.",
            extra={"props": {"doc_id": document_id}},
        )
        errors = []
        return errors

    errors = [
        rename(
            existing_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}.{suffix}"
            ),
            rename_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.{suffix}"
            ),
        )
        for prefix, suffix in [
            (update_config.parser_input, "json"),
            (update_config.embeddings_input, "json"),
            (update_config.indexer_input, "json"),
            (update_config.indexer_input, "npy"),
        ]
    ]
    return errors


def update_dont_parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[str]:
    """
    Update the json objects and remove the npy file in the s3 pipeline cache.

    This is done so that the npy file of embeddings is recreated to reflect the change in the json object field and
    incorporated into the corpus during the next pipeline run whilst not triggering re-parsing of the document.
    """
    document_id, document_update = update
    _LOGGER.info(
        "Updating document so as to not reparse.",
        extra={
            "props": {
                "document_id": document_id,
            }
        },
    )
    errors = [
        update_file_field(
            document_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.json"
            ),
            field=str(document_update.type.value),
            new_value=document_update.csv_value,
            existing_value=document_update.db_value,
        )
        for prefix in [
            update_config.parser_input,
            update_config.embeddings_input,
            update_config.indexer_input,
        ]
    ]

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    errors.append(
        rename(
            existing_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.embeddings_input}/{document_id}.npy"
            ),
            rename_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{update_config.embeddings_input}/{document_id}/{timestamp}.npy"
            ),
        )
    )
    return errors


def parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[str]:
    """
    Update the fields in the json objects to reflect the change made to the data.

    Then remove the desired files by moving them to an archive directory in s3 to re-trigger parsing of the document.
    """
    document_id, document_update = update
    _LOGGER.info(
        "Updating document so as to parse during the next run.",
        extra={
            "props": {
                "document_id": document_id,
            }
        },
    )

    errors = update_dont_parse(update, update_config)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    [
        errors.append(
            rename(
                existing_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.{suffix}"
                ),
                rename_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}/{update_config.archive_trigger_parser}/{prefix}/{document_id}/{timestamp}.{suffix}"
                ),
            )
        )
        for prefix, suffix in [
            (update_config.embeddings_input, "json"),
            (update_config.indexer_input, "json"),
            (update_config.indexer_input, "npy"),
        ]
    ]
    return errors


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
                    "document_path": str(document_path),
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
                        "document_path": str(document_path),
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
                        "document_path": str(document_path),
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
                    "document_path": str(document_path),
                }
            },
        )
        return "NotFoundError: Expected to update document but it doesn't exist."


def rename(existing_path: S3Path, rename_path: S3Path) -> Union[str, None]:
    """Rename the document to the new path."""
    try:
        if existing_path.exists():
            existing_path.rename(rename_path)
            _LOGGER.info(
                "Document renamed.",
                extra={
                    "props": {
                        "document_path": str(existing_path),
                        "archive_path": str(rename_path),
                    }
                },
            )
        else:
            _LOGGER.info(
                "Document does not exist.",
                extra={
                    "props": {
                        "document_path": str(existing_path),
                    }
                },
            )
    except Exception as e:
        _LOGGER.exception(
            "Renaming document failed.",
            extra={
                "props": {
                    "document_path": str(existing_path),
                    "archive_path": str(rename_path),
                    "error": str(e),
                }
            },
        )
        return str(e)
    return None
