import json
import logging
import traceback
from concurrent.futures import as_completed, Executor
from datetime import datetime
from typing import Generator, List, Union, Tuple

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    UpdateConfig,
    Update,
    UpdateResult,
    UpdateTypes,
    Action,
    PipelineFieldMapping,
    UpdateTypeActions,
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

    # TODO do we need a key error try catch here?
    actions = [
        Action(action=UpdateTypeActions(UpdateTypes(update.type)), update=update)
        for update in updates
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


# TODO unless we add more actions this is overkill
def order_actions(actions: List[Action]) -> List[Action]:
    """
    Order the update actions to be performed on an s3 document based upon the action type.

    We need to ensure that we make object updates before archiving a document.
    """
    ordering = [
        update_dont_parse.__name__,
        parse.__name__,
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


def update_dont_parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
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
                f"s3://{update_config.pipeline_bucket}/{update_config.indexer_input}/{document_id}.npy"
            ),
            rename_path=S3Path(
                f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{update_config.indexer_input}/{document_id}/{timestamp}.npy"
            ),
        )
    )
    return [error for error in errors if error is not None]


def parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
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

    update_dont_parse_errors = update_dont_parse(update, update_config)
    errors = [] if update_dont_parse_errors is None else update_dont_parse_errors

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    [
        errors.append(
            rename(
                existing_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}/{prefix}/{document_id}.{suffix}"
                ),
                rename_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}.{suffix}"
                ),
            )
        )
        for prefix, suffix in [
            (update_config.embeddings_input, "json"),
            (update_config.indexer_input, "json"),
            (update_config.indexer_input, "npy"),
        ]
    ]
    return [error for error in errors if error is not None]


def update_file_field(
    document_path: S3Path,
    field: str,
    new_value: Union[str, datetime],
    existing_value: Union[str, datetime],
) -> Union[str, None]:
    """Update the value of a field in a json object within s3 with the new value."""
    if document_path.exists():
        pipeline_field = PipelineFieldMapping[UpdateTypes(field)]
        _LOGGER.info(
            "Updating document field.",
            extra={
                "props": {
                    "document_path": str(document_path),
                    "field": field,
                    "pipeline_field": pipeline_field,
                    "value": new_value,
                    "existing_value": existing_value,
                }
            },
        )
        document = json.loads(document_path.read_text())

        try:
            assert str(document[pipeline_field]) == str(existing_value)
        except AssertionError:
            _LOGGER.error(
                "Field value mismatch - expected value not found in s3 object.",
                extra={
                    "props": {
                        "document_path": str(document_path),
                        "field": field,
                        "pipeline_field": pipeline_field,
                        "value": new_value,
                        "existing_value": existing_value,
                        "document": document,
                    }
                },
            )
            return "FieldMismatchError: Expected value not found in s3 object."
        except KeyError:
            _LOGGER.error(
                "Field not found in s3 object.",
                extra={
                    "props": {
                        "document_path": str(document_path),
                        "field": field,
                        "pipeline_field": pipeline_field,
                        "value": new_value,
                        "existing_value": existing_value,
                        "document": document,
                    }
                },
            )
            return traceback.format_exc()
        document[pipeline_field] = new_value
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
