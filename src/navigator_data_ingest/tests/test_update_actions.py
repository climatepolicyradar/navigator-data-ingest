from cloudpathlib import S3Path

from navigator_data_ingest.base.types import Action
from navigator_data_ingest.base.updated_document_actions import (
    identify_action,
    update_dont_parse,
    archive,
    publish,
    order_actions,
    parse,
    get_latest_timestamp,
)


def test_identify_action_function(test_updates):
    """Test the identify_action function returns the correct callable (function) given an UpdateResult."""

    assert identify_action(test_updates[0]) == update_dont_parse
    assert identify_action(test_updates[1]) == parse
    assert identify_action(test_updates[2]) == publish


def test_order_actions_function(test_updates):
    """Test the order_actions function returns the correct order of actions given a list of actions."""
    actions = [
        Action(action=archive, update=test_updates[0]),
        Action(action=publish, update=test_updates[0]),
        Action(action=update_dont_parse, update=test_updates[0]),
    ]

    assert order_actions(actions) == [
        Action(action=publish, update=test_updates[0]),
        Action(action=update_dont_parse, update=test_updates[0]),
        Action(action=archive, update=test_updates[0]),
    ]


def test_archive_function(
    test_s3_client,
    test_update_config,
    s3_document_keys,
    archive_file_pattern,
    s3_document_id,
    test_updates,
):
    """Test the archive function effectively archives a document."""
    errors = [
        error
        for error in archive(
            update=(
                s3_document_id,
                test_updates[2],
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


def test_get_latest_timestamp_empty_archive(
    test_s3_client, s3_bucket_and_region, test_update_config, s3_document_id
):
    latest_timestamp = get_latest_timestamp(
        document_id=s3_document_id, update_config=test_update_config
    )

    assert latest_timestamp is None


def test_get_latest_timestamp_filled_archive(
    test_s3_client_filled_archive,
    s3_bucket_and_region,
    test_update_config,
    s3_document_id,
):
    latest_timestamp = get_latest_timestamp(
        document_id=s3_document_id, update_config=test_update_config
    )

    assert latest_timestamp == "2023-01-21-01-12-12"


def test_publish(
    test_s3_client_filled_archive, s3_document_id, test_update_config, test_updates
):
    errors = [
        error
        for error in publish(
            update=(
                s3_document_id,
                test_updates[2],
            ),
            update_config=test_update_config,
        )
        if not "None"
    ]

    assert errors == []

    archived_files = list(
        S3Path(
            f"s3://{test_update_config.pipeline_bucket}/{test_update_config.archive_prefix}/"
        ).glob("*/*/*")
    )
    published_files = list(
        S3Path(
            f"s3://{test_update_config.pipeline_bucket}/{test_update_config.parser_input}/"
        ).glob("*")
    )
    assert len(archived_files) == 3
    assert len(published_files) == 1


def test_update_file_field():
    pass


def test_rename():
    pass


def test_update_dont_parse(
    test_s3_client, test_update_config, test_updates, s3_document_id
):
    errors = [
        error
        for error in update_dont_parse(
            update=(s3_document_id, test_updates[0]), update_config=test_update_config
        )
        if not "None"
    ]

    assert errors == []

    parser_input_files = list(
        S3Path(
            f"s3://{test_update_config.pipeline_bucket}/{test_update_config.parser_input}/"
        ).glob("*")
    )
    assert len(parser_input_files) == 1

    embeddings_input_files = list(
        S3Path(
            f"s3://{test_update_config.pipeline_bucket}/{test_update_config.embeddings_input}/"
        ).glob("*")
    )
    assert len(embeddings_input_files) == 1

    assert (
        len(
            list(
                S3Path(
                    f"s3://{test_update_config.pipeline_bucket}/{test_update_config.indexer_input}/"
                ).glob("*")
            )
        )
        == 2
    )

    # TODO assert that actual data in the fields


def test_parse(test_s3_client, test_update_config, test_updates, s3_document_id):
    errors = [
        error
        for error in parse(
            update=(s3_document_id, test_updates[1]), update_config=test_update_config
        )
        if not "None"
    ]

    assert errors == []


def test_utils():
    pass
