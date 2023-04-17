import json
import logging
import os
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
)

_LOGGER = logging.getLogger(__file__)


METADATA_KEY = os.environ.get("METADATA_KEY", "document_metadata")


# TODO: hard coding translated language will lead to issues if we have more target languages in future
def get_document_files(
    prefix_path: S3Path, document_id: str, suffix_filter: str
) -> List[S3Path]:
    """Get the document files for a given document ID found in an s3 directory."""
    return [
        prefix_path / f"{document_id}.{suffix_filter}",
        prefix_path / f"{document_id}_translated_en.{suffix_filter}",
    ]


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
        Action(action=update_type_actions[UpdateTypes(update.type)], update=update)
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


def order_actions(actions: List[Action]) -> List[Action]:
    """
    Order the update actions to be performed on an s3 document based upon the action type.

    We need to ensure that we make object updates in a particular order.

    If the action is to parse then we only perform this action.
    """
    for action in actions:
        if action.action == parse:
            return [action]

    # TODO: Currently only two actions other than 'parse' are 'update_dont_parse' and 'publication_ts'. We want to
    #  update publication_ts before update_dont_parse as we don't want to attempt update of a document that is
    #  archived during the update_dont_parse process. Thus, we order the actions so that 'publication_ts' is first.
    def get_action_priority(action_name: str) -> int:
        """Get the priority of an action."""
        return 1 if action_name == update_dont_parse.__name__ else 0

    return [
        action
        for action in sorted(
            actions, key=lambda action: get_action_priority(action.action.__name__)
        )
    ]


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
    errors = []
    for prefix_path in [
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.parser_input
            )
        ),
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.embeddings_input
            )
        ),
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.indexer_input
            )
        ),
    ]:
        # Might be translated and non-translated json objects
        document_files = get_document_files(
            prefix_path, document_id, suffix_filter="json"
        )
        for document_file in document_files:
            errors.append(
                update_file_field(
                    document_path=document_file,
                    field=str(document_update.type.value),
                    new_value=document_update.db_value,
                    existing_value=document_update.s3_value,
                )
            )

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    errors.append(
        rename(
            existing_path=S3Path(
                os.path.join(
                    "s3://",
                    update_config.pipeline_bucket,
                    update_config.indexer_input,
                    f"{document_id}.npy",
                )
            ),
            rename_path=S3Path(
                os.path.join(
                    "s3://",
                    update_config.pipeline_bucket,
                    update_config.archive_prefix,
                    update_config.indexer_input,
                    document_id,
                    f"{timestamp}.npy",
                )
            ),
        )
    )
    return [error for error in errors if error is not None]


def parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
    """Archive all instances of the document in the s3 pipeline cache to trigger full re-processing."""
    document_id, document_update = update
    _LOGGER.info(
        "Archiving document so as to re-download from source and parse during the next run.",
        extra={
            "props": {
                "document_id": document_id,
            }
        },
    )
    errors = []

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    for prefix in [
        update_config.parser_input,
        update_config.embeddings_input,
        update_config.indexer_input,
    ]:
        prefix_path = S3Path(
            os.path.join("s3://", update_config.pipeline_bucket, prefix)
        )

        # Might be translated and non-translated json objects
        document_files = get_document_files(
            prefix_path, document_id, suffix_filter="json"
        ) + get_document_files(prefix_path, document_id, suffix_filter="npy")
        for document_file in document_files:
            errors.append(
                rename(
                    existing_path=document_file,
                    rename_path=S3Path(
                        f"s3://{update_config.pipeline_bucket}/{update_config.archive_prefix}/{prefix}/{document_id}/{timestamp}{document_file.suffix}"
                    ),
                )
            )

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
            if not str(document[pipeline_field]) == str(existing_value):
                _LOGGER.info(
                    "Existing value doesn't match.",
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

            document[pipeline_field] = new_value
        except KeyError:
            _LOGGER.exception(
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

        document_path.write_text(json.dumps(document))
        return None
    else:
        _LOGGER.info(
            "Tried to update document but it doesn't exist.",
            extra={
                "props": {
                    "document_path": str(document_path),
                }
            },
        )
        # TODO: convert to an f-string with more details when we can identify the expected files
        # return "NotFoundError: Expected to update document but it doesn't exist."
        return None


def update_publication_ts(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
    """Update the value of the publication_ts field in all json objects for a document within s3 with the new value."""
    document_id, document_update = update
    _LOGGER.info(
        "Updating publication_ts for document instances in s3.",
        extra={
            "props": {
                "document_id": document_id,
            }
        },
    )
    errors = []
    for prefix_path in [
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.parser_input
            )
        ),
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.embeddings_input
            )
        ),
        S3Path(
            os.path.join(
                "s3://", update_config.pipeline_bucket, update_config.indexer_input
            )
        ),
    ]:
        # Might be translated and non-translated json objects
        document_files = get_document_files(
            prefix_path, document_id, suffix_filter="json"
        )
        for document_file in document_files:
            errors.append(
                update_file_metadata_field(
                    document_path=document_file,
                    metadata_field=str(document_update.type.value),
                    new_value=document_update.db_value,
                    existing_value=document_update.s3_value,
                )
            )
    return [error for error in errors if error is not None]


def update_file_metadata_field(
    document_path: S3Path,
    metadata_field: str,
    new_value: Union[str, datetime],
    existing_value: Union[str, datetime],
) -> Union[str, None]:
    """Update the value of a metadata field in a json object within s3 with the new value."""
    if document_path.exists():
        pipeline_metadata_field = PipelineFieldMapping[UpdateTypes(metadata_field)]
        _LOGGER.info(
            "Updating document metadata field.",
            extra={
                "props": {
                    "document_path": str(document_path),
                    "metadata_field": metadata_field,
                    "pipeline_field": pipeline_metadata_field,
                    "value": new_value,
                }
            },
        )
        document = json.loads(document_path.read_text())

        try:
            if not str(document[METADATA_KEY][pipeline_metadata_field]) == str(
                existing_value
            ):
                _LOGGER.info(
                    "Existing value doesn't match.",
                    extra={
                        "props": {
                            "document_path": str(document_path),
                            "metadata_field": metadata_field,
                            "pipeline_field": pipeline_metadata_field,
                            "value": new_value,
                            "existing_value": existing_value,
                            "document": document,
                        }
                    },
                )

            document[METADATA_KEY][pipeline_metadata_field] = new_value
        except KeyError:
            _LOGGER.exception(
                "Field not found in s3 object.",
                extra={
                    "props": {
                        "document_path": str(document_path),
                        "metadata_field": metadata_field,
                        "pipeline_field": pipeline_metadata_field,
                        "value": new_value,
                        "document": document,
                    }
                },
            )
            return traceback.format_exc()

        document_path.write_text(json.dumps(document))
        return None
    else:
        _LOGGER.info(
            "Tried to update document but it doesn't exist.",
            extra={
                "props": {
                    "document_path": str(document_path),
                }
            },
        )
        # TODO: convert to an f-string with more details when we can identify the expected files
        # return "NotFoundError: Expected to update document but it doesn't exist."
        return None


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


update_type_actions = {
    UpdateTypes.SOURCE_URL: parse,
    UpdateTypes.NAME: update_dont_parse,
    UpdateTypes.DESCRIPTION: update_dont_parse,
    UpdateTypes.PUBLICATION_TS: update_publication_ts,
}
