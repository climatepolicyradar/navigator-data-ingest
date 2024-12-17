import json
import logging
import os
import traceback
from concurrent.futures import Executor, as_completed
from datetime import datetime
from typing import Generator, List, Tuple, Union

from cloudpathlib import S3Path
from cpr_sdk.pipeline_general_models import Update, UpdateTypes

from navigator_data_ingest.base.types import (
    Action,
    PipelineFieldMapping,
    UpdateConfig,
    UpdateResult,
)

_LOGGER = logging.getLogger(__file__)


# TODO: hard coding translated language will lead to issues if we have more target
#  languages in future, this could be solved by defining target languages in the DAL.
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

    For each document: Iterate through the document updates and perform the relevant
    action based upon the update type.
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
    Order update actions to be performed on an s3 document based upon the action type.

    We need to ensure that we make object updates in a particular order.

    If the action is to parse then we only perform this action.
    """
    for action in actions:
        if action.action == parse:
            return [action]

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

    This is done so that the npy file of embeddings is recreated to reflect the change
    in the json object field and incorporated into the corpus during the next pipeline
    run whilst not triggering re-parsing of the document.
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
    ]:
        # Might be translated and non-translated json objects
        document_files = get_document_files(
            prefix_path, document_id, suffix_filter="json"
        )
        for document_file in document_files:
            error = update_file_field(
                document_path=document_file,
                field=str(document_update.type.value),
                new_value=document_update.db_value,
                existing_value=document_update.s3_value,
            )
            if error:
                errors.append(error)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Archive npy file
    archive_npy_error = rename(
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
    if archive_npy_error:
        errors.append(archive_npy_error)

    # Archive json file
    archive_json_error = rename(
        existing_path=S3Path(
            os.path.join(
                "s3://",
                update_config.pipeline_bucket,
                update_config.indexer_input,
                f"{document_id}.json",
            )
        ),
        rename_path=S3Path(
            os.path.join(
                "s3://",
                update_config.pipeline_bucket,
                update_config.archive_prefix,
                update_config.indexer_input,
                document_id,
                f"{timestamp}.json",
            )
        ),
    )
    if archive_json_error:
        errors.append(archive_json_error)

    return errors


def parse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
    """
    Archive all instances of the document in the s3 pipeline cache.

    This is done to trigger full re-processing.
    """
    document_id, document_update = update
    _LOGGER.info(
        "Archiving document so as to re-download from source and parse during the next "
        "run.",
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
            error = rename(
                existing_path=document_file,
                rename_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}"
                    f"/{update_config.archive_prefix}"
                    f"/{prefix}/{document_id}"
                    f"/{timestamp}{document_file.suffix} "
                ),
            )
            if error:
                errors.append(error)

    return errors


def reparse(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
    """
    Archive instances of the document in the pre-parser and pre-embeddings stages.

    This is done to trigger the parsing and embeddings generation stages of the
    pipeline on the document but not the re-download from source.
    """
    document_id, document_update = update
    _LOGGER.info(
        "Archiving pre-parser and pre-embeddings instatiations of the document so as to "
        "parse during the next run.",
        extra={
            "props": {
                "document_id": document_id,
            }
        },
    )
    errors = []

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    for prefix in [
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
            error = rename(
                existing_path=document_file,
                rename_path=S3Path(
                    f"s3://{update_config.pipeline_bucket}"
                    f"/{update_config.archive_prefix}"
                    f"/{prefix}/{document_id}"
                    f"/{timestamp}{document_file.suffix} "
                ),
            )
            if error:
                errors.append(error)

    return errors


def update_field_in_all_occurences(
    update: Tuple[str, Update],
    update_config: UpdateConfig,
) -> List[Union[str, None]]:
    """Update the document slug in all occurences of the document in s3."""
    # TODO Do we need to archive on slug updates? The reason for this was expensive
    # translation and text extraction costs not slug updates?
    # TODO This can be made more generic.

    document_id, document_update = update
    _LOGGER.info(
        "Updating document field in all document occurences in s3.",
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
        document_files = get_document_files(
            prefix_path, document_id, suffix_filter="json"
        )
        for document_file in document_files:
            error = update_file_field(
                document_path=document_file,
                field=str(document_update.type.value),
                new_value=document_update.db_value,
                existing_value=document_update.s3_value,
            )
            if error:
                errors.append(error)
    return errors


def update_file_field(
    document_path: S3Path,
    field: str,
    new_value: Union[str, datetime, dict, None],
    existing_value: Union[str, datetime, dict, None],
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
        # TODO: convert to an f-string with more details when we can identify the
        #  expected files return "NotFoundError: Expected to update document but it
        #  doesn't exist."
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
    UpdateTypes.REPROCESS: parse,
    UpdateTypes.NAME: update_dont_parse,
    UpdateTypes.DESCRIPTION: update_dont_parse,
    UpdateTypes.METADATA: update_dont_parse,
    UpdateTypes.SLUG: update_field_in_all_occurences,
    UpdateTypes.REPARSE: reparse,
}
