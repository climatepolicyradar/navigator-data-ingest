"""Base definitions for data ingest"""
import os
from abc import abstractmethod, ABC
from csv import DictReader
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Generator, Mapping, Optional, Sequence, Union

from pydantic import BaseModel

# if the source data doesn't have descriptions for things like events, or
# non-lookup/predefined metadata, use a suitable default.
DEFAULT_DESCRIPTION = "Imported by CPR loader"
DEFAULT_POLICY_DATE = datetime(1900, 1, 1)
PUBLICATION_EVENT_NAME = "Publication"

SINGLE_FILE_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}
ADDITIONAL_SUPPORTED_CONTENT_TYPES = {"text/html"}
SUPPORTED_CONTENT_TYPES = SINGLE_FILE_CONTENT_TYPES | ADDITIONAL_SUPPORTED_CONTENT_TYPES


def _get_data_dir() -> Path:
    data_dir = os.environ.get("DATA_DIR")
    if data_dir is not None:
        data_dir = Path(data_dir).resolve()
    else:
        data_dir = Path(__file__).parent.resolve() / "data"
    return data_dir


def _load_geography_mapping() -> Mapping[str, str]:
    source_path = _get_data_dir() / GEOGRAPHY_DATA_FILE
    geography_mapping = {}
    with open(source_path) as geography_source:
        csv_reader = DictReader(geography_source)
        for row in csv_reader:
            geography_mapping[row["Name"]] = row["Iso"]
    return geography_mapping


GEOGRAPHY_DATA_FILE = "geography-iso-3166.csv"
GEOGRAPHY_ISO_LOOKUP = _load_geography_mapping()


class Event(BaseModel):  # noqa: D101
    """
    A representation of events associated with a document.
    """
    name: str
    description: str
    date: datetime


class Document(BaseModel):
    """
    A representation of all information expected to be provided for a document.

    This class comprises direct information describing a document, along
    with all metadata values that should be associated with that document.
    """
    name: str
    description: str
    source_url: str  # Original source URL
    source_id: str  # Unique source derived ID
    publication_ts: datetime
    url: Optional[str]  # Serving URL returned from CDN upload
    md5sum: Optional[str]  # md5sum calculated during upload

    type: str
    source: str
    category: str
    geography: str

    frameworks: Sequence[str]
    instruments: Sequence[str]
    hazards: Sequence[str]
    keywords: Sequence[str]
    languages: Sequence[str]
    sectors: Sequence[str]
    topics: Sequence[str]

    events: Sequence[Event]


class DocumentRelationship(BaseModel):
    """A representation of a relationship between multiple documents."""
    name: str
    description: str
    type: str


class DocumentGenerator(ABC):

    @abstractmethod
    def process_source(self) -> Generator[Sequence[Document], None, None]:
        raise NotImplementedError("process_source() not implemented")


class DocumentType(Enum, str):

    LAW = "Law"
    POLICY = "Policy"


class DocumentUploadResult(BaseModel):
    """Information generated during the upload of a document used by later processes."""

    cloud_url: str
    md5sum: str