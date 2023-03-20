from cloudpathlib import S3Path
import json

from navigator_data_ingest.base.types import Action, PipelineFieldMapping, UpdateTypes
from navigator_data_ingest.base.updated_document_actions import (
    identify_action,
    update_dont_parse,
    archive,
    publish,
    order_actions,
    parse,
    get_latest_timestamp,
    update_file_field,
    rename,
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
    publish_document_update = test_updates[2]

    errors = [
        error
        for error in archive(
            update=(
                s3_document_id,
                publish_document_update,
            ),
            update_config=test_update_config,
        )
        if not "None"
    ]

    assert errors == []
    (
        parser_input_doc,
        embeddings_input_doc,
        indexer_input_doc_json,
        indexer_input_doc_npy,
    ) = [
        S3Path(f"s3://{test_update_config.pipeline_bucket}/{s3_key}")
        for s3_key in s3_document_keys
    ]

    assert not parser_input_doc.exists()
    assert not embeddings_input_doc.exists()
    assert not indexer_input_doc_json.exists()
    assert not indexer_input_doc_npy.exists()

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
    """Test the get_latest_timestamp function returns None if there are no archived documents."""
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
    """Test the get_latest_timestamp function returns the latest timestamp from the s3 object keys."""
    latest_timestamp = get_latest_timestamp(
        document_id=s3_document_id, update_config=test_update_config
    )

    assert latest_timestamp == "2023-01-21-01-12-12"


def test_publish(
    test_s3_client_filled_archive, s3_document_id, test_update_config, test_updates
):
    """Test the publish function effectively publishes a document."""
    publish_document_update = test_updates[2]

    errors = [
        error
        for error in publish(
            update=(
                s3_document_id,
                publish_document_update,
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
    # Four files in the parser archive directory. Assert that we only publish one file.
    assert len(archived_files) == 3
    assert len(published_files) == 1


def test_update_file_field(
    test_s3_client,
    s3_bucket_and_region,
    test_update_config,
    s3_document_id,
    parser_input_json,
):
    """Test the update_file_field function effectively updates a field in an s3 json object."""
    parser_input_document_path = S3Path(
        f"s3://{test_update_config.pipeline_bucket}/{test_update_config.parser_input}/{s3_document_id}.json"
    )

    error = update_file_field(
        document_path=parser_input_document_path,
        field="document_name",
        new_value="new document name",
        existing_value=parser_input_json["document_name"],
    )

    document_post_update = json.loads(parser_input_document_path.read_text())

    assert error is None
    assert document_post_update["document_name"] == "new document name"


def test_rename(
    test_s3_client, test_update_config, s3_bucket_and_region, s3_document_keys
):
    """Test the rename function effectively renames an s3 object."""
    existing_path = S3Path(
        f"s3://{test_update_config.pipeline_bucket}/{s3_document_keys[0]}"
    )

    rename_path = S3Path(
        f"s3://{test_update_config.pipeline_bucket}/test-prefix/test-document-id.json"
    )

    error = rename(existing_path=existing_path, rename_path=rename_path)

    assert error is None
    assert not s3_document_keys[0].exists()
    assert rename_path.exists()


def test_update_dont_parse(
    test_s3_client, test_update_config, test_updates, s3_document_id, s3_document_keys
):
    """Test the update_dont_parse function effectively updates a document such that it isn't parsed in the pipeline."""
    update_to_document_name = test_updates[0]

    errors = [
        error
        for error in update_dont_parse(
            update=(s3_document_id, update_to_document_name),
            update_config=test_update_config,
        )
        if not "None"
    ]

    assert errors == []

    (
        parser_input_doc,
        embeddings_input_doc,
        indexer_input_doc_json,
        indexer_input_doc_npy,
    ) = [
        S3Path(f"s3://{test_update_config.pipeline_bucket}/{s3_key}")
        for s3_key in s3_document_keys
    ]

    assert parser_input_doc.exists()
    assert embeddings_input_doc.exists()
    assert indexer_input_doc_json.exists()
    assert not indexer_input_doc_npy.exists()

    parser_input_doc_data = json.loads(parser_input_doc.read_text())
    assert (
        parser_input_doc_data[
            PipelineFieldMapping[UpdateTypes(update_to_document_name.type)]
        ]
        == update_to_document_name.csv_value
    )

    embeddings_input_doc_data = json.loads(embeddings_input_doc.read_text())
    assert (
        embeddings_input_doc_data[
            PipelineFieldMapping[UpdateTypes(update_to_document_name.type)]
        ]
        == update_to_document_name.csv_value
    )

    indexer_input_doc_json_data = json.loads(indexer_input_doc_json.read_text())
    assert (
        indexer_input_doc_json_data[
            PipelineFieldMapping[UpdateTypes(update_to_document_name.type)]
        ]
        == update_to_document_name.csv_value
    )


def test_parse(
    test_s3_client, test_update_config, test_updates, s3_document_keys, s3_document_id
):
    """Test that a document is updated such that it is parsed by the pipeline."""
    update_to_source_url = test_updates[1]

    errors = [
        error
        for error in parse(
            update=(s3_document_id, update_to_source_url),
            update_config=test_update_config,
        )
        if not "None"
    ]

    assert errors == []

    (
        parser_input_doc,
        embeddings_input_doc,
        indexer_input_doc_json,
        indexer_input_doc_npy,
    ) = [
        S3Path(f"s3://{test_update_config.pipeline_bucket}/{s3_key}")
        for s3_key in s3_document_keys
    ]

    assert parser_input_doc.exists()
    assert not embeddings_input_doc.exists()
    assert not indexer_input_doc_json.exists()
    assert not indexer_input_doc_npy.exists()

    parser_input_doc_data = json.loads(parser_input_doc.read_text())
    assert (
        parser_input_doc_data[
            PipelineFieldMapping[UpdateTypes(update_to_source_url.type)]
        ]
        == update_to_source_url.csv_value
    )
