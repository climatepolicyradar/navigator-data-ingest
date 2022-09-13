import logging
from typing import Generator, Sequence

from src.base.types import Document

_LOGGER = logging.getLogger(__name__)


def group_documents(
    document_generator: Generator[Document, None, None],
) -> Generator[Sequence[Document], None, None]:
    last_action_id = None
    document_group = []

    for document in document_generator:
        action_id = document.source_id.split("-")[0]
        if last_action_id is None or last_action_id == action_id:
            document_group.append(document)
        else:
            yield document_group
            document_group = [document]

        last_action_id = action_id

    if document_group:
        yield document_group
