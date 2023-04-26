"""Base definitions for data ingest"""
from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Generator,
    Mapping,
    Optional,
    Sequence,
    Union,
    List,
    Callable,
)

from pydantic import AnyHttpUrl, BaseModel

CONTENT_TYPE_PDF = "application/pdf"
CONTENT_TYPE_DOCX = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
CONTENT_TYPE_HTML = "text/html"

SINGLE_FILE_CONTENT_TYPES = {
    CONTENT_TYPE_PDF,
    CONTENT_TYPE_DOCX,
}
MULTI_FILE_CONTENT_TYPES = {CONTENT_TYPE_HTML}
SUPPORTED_CONTENT_TYPES = SINGLE_FILE_CONTENT_TYPES | MULTI_FILE_CONTENT_TYPES


class DocumentType(str, Enum):
    """Document types supported by the backend API."""

    LAW = "Law"
    POLICY = "Policy"
    LITIGATION = "Litigation"


CATEGORY_MAPPING = {
    "executive": DocumentType.POLICY,
    "legislative": DocumentType.LAW,
    "litigation": DocumentType.LITIGATION,
}
FILE_EXTENSION_MAPPING = {
    CONTENT_TYPE_PDF: ".pdf",
    CONTENT_TYPE_HTML: ".html",
    CONTENT_TYPE_DOCX: ".docx",
}


class Event(BaseModel):  # noqa: D101
    """A representation of events associated with a document."""

    name: str
    description: str
    created_ts: datetime

    def to_json(self) -> Mapping[str, Any]:
        """Output a JSON serialising friendly dict representing this model"""
        return {
            "name": self.name,
            "description": self.description,
            "created_ts": self.created_ts.isoformat(),
        }


class Document(BaseModel):
    """
    A representation of all information expected to be provided for a document.

    This class comprises direct information describing a document, along
    with all metadata values that should be associated with that document.
    """

    name: str
    description: str
    import_id: str  # Unique source derived ID
    slug: str  # Unique identifier created by backend
    publication_ts: datetime
    source_url: Optional[AnyHttpUrl]  # Original source URL

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

    def to_json(self) -> Mapping[str, Any]:
        """Output a JSON serialising friendly dict representing this model"""
        json_dict = self.dict()
        json_dict["publication_ts"] = self.publication_ts.isoformat()
        json_dict["events"] = [event.to_json() for event in self.events]
        return json_dict


class UpdateTypes(str, Enum):
    """Document types supported by the backend API."""

    NAME = "name"
    DESCRIPTION = "description"
    # IMPORT_ID = "import_id"
    # SLUG = "slug"
    PUBLICATION_TS = "publication_ts"
    SOURCE_URL = "source_url"
    # TYPE = "type"
    # SOURCE = "source"
    # CATEGORY = "category"
    # GEOGRAPHY = "geography"
    # FRAMEWORKS = "frameworks"
    # INSTRUMENTS = "instruments"
    # HAZARDS = "hazards"
    # KEYWORDS = "keywords"
    # LANGUAGES = "languages"
    # SECTORS = "sectors"
    # TOPICS = "topics"
    # EVENTS = "events"
    # DOCUMENT_STATUS = "document_status"


PipelineFieldMapping = {
    UpdateTypes.NAME: "document_name",
    UpdateTypes.DESCRIPTION: "document_description",
    UpdateTypes.SOURCE_URL: "document_source_url",
    UpdateTypes.PUBLICATION_TS: "publication_ts",
}


class DocumentParserInput(BaseModel):
    """Input specification for input to the parser."""

    document_id: str
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl]
    document_cdn_object: Optional[str] = None
    document_content_type: Optional[str] = None
    document_md5_sum: Optional[str] = None
    document_metadata: Document
    document_slug: str

    def to_json(self) -> Mapping[str, Any]:
        """Output a JSON serialising friendly dict representing this model"""
        return {
            "document_name": self.document_name,
            "document_description": self.document_description,
            "document_id": self.document_id,
            "document_source_url": self.document_source_url,
            "document_cdn_object": self.document_cdn_object,
            "document_content_type": self.document_content_type,
            "document_md5_sum": self.document_md5_sum,
            "document_metadata": self.document_metadata.to_json(),
            "document_slug": self.document_slug,
        }


class UploadResult(BaseModel):
    """Information generated during the upload of a document used by later processes"""

    cdn_object: Optional[str]
    md5_sum: Optional[str]
    content_type: Optional[str]


class HandleResult(BaseModel):
    """Result of handling an input file"""

    parser_input: DocumentParserInput
    error: Optional[str] = None


class UnsupportedContentTypeError(Exception):
    """An error indicating a content type not yet supported by parsing"""

    def __init__(self, content_type: str):
        self.content_type = content_type
        super().__init__(f"Content type '{content_type}' is not supported for caching")


class UpdateDefinition(BaseModel):
    """Class describing the results of comparing csv data against the db data to identify updates."""

    db_value: Union[str, datetime]
    s3_value: Union[str, datetime]
    type: UpdateTypes


class UpdateResult(BaseModel):
    """Result of updating a document update via the ingest stage."""

    document_id: str
    update: UpdateDefinition
    error: Optional[str] = None


class InputData(BaseModel):
    """Expected input data containing both document updates and new documents for the ingest stage of the pipeline."""

    new_documents: List[Document]
    updated_documents: dict[str, List[UpdateDefinition]]


@dataclass
class UpdateConfig:
    """Shared configuration for document update functions."""

    pipeline_bucket: str
    input_prefix: str
    parser_input: str
    embeddings_input: str
    indexer_input: str
    archive_prefix: str


class DocumentGenerator(ABC):
    """Base class for all document sources."""

    @abstractmethod
    def process_new_documents(self) -> Generator[Document, None, None]:
        """Generate new documents for processing from the configured source"""

        raise NotImplementedError("process_new_documents() not implemented")

    @abstractmethod
    def process_updated_documents(self) -> Generator[UpdateDefinition, None, None]:
        """Generate documents with updates for processing from the configured source"""

        raise NotImplementedError("process_updated_documents() not implemented")


class Action(BaseModel):
    """Base class for associating an update with the relevant action."""

    update: UpdateDefinition
    action: Callable
