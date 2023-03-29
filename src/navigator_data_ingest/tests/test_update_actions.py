from cloudpathlib import S3Path
import json
import pytest

from navigator_data_ingest.base.types import (
    Action,
    PipelineFieldMapping,
    UpdateTypes,
    UpdateTypeActions,
)
from navigator_data_ingest.base.updated_document_actions import (
    update_dont_parse,
    order_actions,
    parse,
    update_file_field,
    rename,
)


@pytest.mark.unit
def test_identify_action_function(test_updates):
    """Test the UpdateTypeActions mapping returns the correct callable (function) given an UpdateResult."""

    assert UpdateTypeActions(test_updates[0].type) == update_dont_parse
    assert UpdateTypeActions(test_updates[2].type) == parse


@pytest.mark.unit
def test_order_actions_function(test_updates):
    """Test the order_actions function returns the correct order of actions given a list of actions."""
    actions = [
        Action(action=parse, update=test_updates[0]),
        Action(action=update_dont_parse, update=test_updates[0]),
    ]

    assert order_actions(actions) == [
        Action(action=update_dont_parse, update=test_updates[0]),
        Action(action=parse, update=test_updates[0]),
    ]


@pytest.mark.unit
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
        field="name",
        new_value="new document name",
        existing_value=parser_input_json["document_name"],
    )

    document_post_update = json.loads(parser_input_document_path.read_text())

    assert error is None
    assert document_post_update["document_name"] == "new document name"


@pytest.mark.unit
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
    assert not existing_path.exists()
    assert rename_path.exists()


@pytest.mark.unit
def test_update_dont_parse(
    test_s3_client, test_update_config, test_updates, s3_document_id, s3_document_keys
):
    """Test the update_dont_parse function effectively updates a document such that it isn't parsed in the pipeline."""
    update_to_document_description = test_updates[1]

    errors = [
        error
        for error in update_dont_parse(
            update=(s3_document_id, update_to_document_description),
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
            PipelineFieldMapping[UpdateTypes(update_to_document_description.type)]
        ]
        == update_to_document_description.csv_value
    )

    embeddings_input_doc_data = json.loads(embeddings_input_doc.read_text())
    assert (
        embeddings_input_doc_data[
            PipelineFieldMapping[UpdateTypes(update_to_document_description.type)]
        ]
        == update_to_document_description.csv_value
    )

    indexer_input_doc_json_data = json.loads(indexer_input_doc_json.read_text())
    assert (
        indexer_input_doc_json_data[
            PipelineFieldMapping[UpdateTypes(update_to_document_description.type)]
        ]
        == update_to_document_description.csv_value
    )


@pytest.mark.unit
def test_parse(
    test_s3_client, test_update_config, test_updates, s3_document_keys, s3_document_id
):
    """Test that a document is updated such that it is parsed by the pipeline."""
    update_to_source_url = test_updates[2]

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
