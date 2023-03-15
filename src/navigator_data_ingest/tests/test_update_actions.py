from cloudpathlib import S3Path

from navigator_data_ingest.base.updated_document_actions import (
    identify_action,
    update_dont_parse,
    archive,
    publish,
    order_actions,
    parse,
)
from navigator_data_ingest.base.types import Update, Action

# TODO make a fixture for this
update_1 = Update(
    type="name",
    csv_value="new name",
    db_value="old name",
)
update_2 = Update(
    type="source_url",
    csv_value="new url",
    db_value="old url",
)
update_3 = Update(
    type="document_status",
    csv_value="PUBLISHED",
    db_value="DELETED",
)


def test_identify_action_function():
    """Test the identify_action function returns the correct callable (function) given an UpdateResult."""

    assert identify_action(update_1) == update_dont_parse
    assert identify_action(update_2) == parse
    assert identify_action(update_3) == publish


def test_order_actions_function():
    """Test the order_actions function returns the correct order of actions given a list of actions."""
    actions = [
        Action(action=archive, update=update_1),
        Action(action=publish, update=update_1),
        Action(action=update_dont_parse, update=update_1),
    ]

    assert order_actions(actions) == [
        Action(action=publish, update=update_1),
        Action(action=update_dont_parse, update=update_1),
        Action(action=archive, update=update_1),
    ]


def test_archive_function(
    test_s3_client,
    test_update_config,
    s3_document_keys,
    archive_file_pattern,
    s3_document_id,
):
    """Test the archive function effectively archives a document."""
    errors = [
        error
        for error in archive(
            update=(
                s3_document_id,
                Update(
                    type="document_status",
                    csv_value="PUBLISHED",
                    db_value="DELETED",
                ),
            ),
            update_config=test_update_config,
        )
        if not "None"
    ]

    assert errors == []
    assert (
        list(
            S3Path(
                f"s3://{test_update_config.pipeline_bucket}/{test_update_config.parser_input}/"
            ).glob("*")
        )
        == []
    )
    assert (
        list(
            S3Path(
                f"s3://{test_update_config.pipeline_bucket}/{test_update_config.embeddings_input}/"
            ).glob("*")
        )
        == []
    )
    assert (
        list(
            S3Path(
                f"s3://{test_update_config.pipeline_bucket}/{test_update_config.indexer_input}/"
            ).glob("*")
        )
        == []
    )

    archived_files = list(
        S3Path(
            f"s3://{test_update_config.pipeline_bucket}/{test_update_config.archive_prefix}/"
        ).glob("*/*/*")
    )
    assert len(archived_files) == len(s3_document_keys)
    for archived_file in archived_files:
        assert archive_file_pattern["json"].match(
            archived_file.name
        ) or archive_file_pattern["npy"].match(archived_file.name)
