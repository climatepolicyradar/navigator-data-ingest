from csv import DictReader
from io import TextIOWrapper
from pathlib import Path
from typing import Generator, Optional, Sequence, Set

from base.types import Document, DocumentGenerator
from origins.cclw.extract import (
    TITLE_FIELD,
    DESCRIPTION_FIELD,
    COUNTRY_CODE_FIELD,
    DOCUMENT_FIELD,
    CATEGORY_FIELD,
    EVENTS_FIELD,
    SECTORS_FIELD,
    INSTRUMENTS_FIELD,
    FRAMEWORKS_FIELD,
    TOPICS_FIELD,
    HAZARDS_FIELD,
    DOCUMENT_TYPE_FIELD,
    YEAR_FIELD,
    LANGUAGES_FIELD,
    KEYWORDS_FIELD,
    GEOGRAPHY_FIELD,
    PARENT_LEGISLATION_FIELD,
    extract_documents,
)
from origins.cclw.transform import group_documents

_EXPECTED_FIELDS = set(
    [
        TITLE_FIELD,
        DESCRIPTION_FIELD,
        COUNTRY_CODE_FIELD,
        DOCUMENT_FIELD,
        CATEGORY_FIELD,
        EVENTS_FIELD,
        SECTORS_FIELD,
        INSTRUMENTS_FIELD,
        FRAMEWORKS_FIELD,
        TOPICS_FIELD,
        HAZARDS_FIELD,
        DOCUMENT_TYPE_FIELD,
        YEAR_FIELD,
        LANGUAGES_FIELD,
        KEYWORDS_FIELD,
        GEOGRAPHY_FIELD,
        PARENT_LEGISLATION_FIELD,
    ]
)


class InputFileError(Exception):
    """Base class for input file exceptions."""


class EmptyInputError(InputFileError):
    """Error raised when an empty input is provided."""

    def __init__(self, file_name: str):
        self.file_name = file_name
        super().__init__(f"The given CSV file '{file_name}' is empty")


class FieldsMismatchError(InputFileError):
    """Error raised when a given CSV does not have the expected fields."""

    def __init__(
        self,
        file_name: str,
        unexpected_fields: Optional[Set[str]] = None,
        missing_fields: Optional[Set[str]] = None,
    ):
        self.file_name = file_name
        self.unexpected_fields = unexpected_fields
        self.missing_fields = missing_fields

        msg = f"The given CSV file '{file_name}' does not contain the expected fields."
        if self.unexpected_fields:
            msg += f"\nUnexpected fields: {unexpected_fields}."
        if self.missing_fields:
            msg += f"\nMissing fields: {missing_fields}."
        super().__init__(msg)


class CCLWDocumentCSV(DocumentGenerator):
    def __init__(self, cclw_input: Path):
        self._input = cclw_input

    def process_source(self) -> Generator[Sequence[Document], None, None]:
        # TODO: Maybe make this fail earlier if we're processing multiple input files
        with open(self._input) as csv_file:
            csv_reader = self._validated_input(csv_file)

            return group_documents(extract_documents(csv_reader))

    def _validated_input(self, csv_file: TextIOWrapper) -> DictReader:
        csv_reader = DictReader(csv_file)

        csv_fields = csv_reader.fieldnames
        if csv_fields is None:
            raise EmptyInputError(csv_file.name)

        unexpected_fields = set(csv_fields) - _EXPECTED_FIELDS
        missing_fields = _EXPECTED_FIELDS - set(csv_fields)
        if unexpected_fields or missing_fields:
            raise FieldsMismatchError(
                csv_file.name,
                unexpected_fields=unexpected_fields,
                missing_fields=missing_fields,
            )

        return csv_reader
