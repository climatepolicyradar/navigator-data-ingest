from navigator_data_ingest.base.updated_document_actions import (
    identify_action,
    update_dont_parse,
    archive,
    publish,
    order_actions,
)
from navigator_data_ingest.base.types import Update, Action


update_1 = Update(
    updated=True,
    type="Family",
    field="name",
    csv_value="new name",
    db_value="old name",
)
update_2 = Update(
    updated=True,
    type="PhysicalDocument",
    field="source_url",
    csv_value="new url",
    db_value="old url",
)
update_3 = Update(
    updated=True,
    type="PhysicalDocument",
    field="document_status",
    csv_value="PUBLISHED",
    db_value="DELETED",
)


def test_identify_action():
    """Test the identify_action function returns the correct callable (function) given an UpdateResult."""

    assert identify_action(update_1) == update_dont_parse
    assert identify_action(update_2) == archive
    assert identify_action(update_3) == publish


def test_order_actions():
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
