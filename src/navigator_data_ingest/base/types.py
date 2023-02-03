"""Base definitions for data ingest"""
from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum
from typing import Any, Generator, Mapping, Optional, Sequence, Literal

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


class DocumentGenerator(ABC):
    """Base class for all document sources."""

    @abstractmethod
    def process_source(self) -> Generator[Document, None, None]:
        """Generate documents for processing from the configured source"""

        raise NotImplementedError("process_source() not implemented")


class DocumentUploadResult(BaseModel):
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


class DocumentUpdate(BaseModel):
    """A definition of updates to be performed on the instances of a document in the pipeline."""
    id: str
    updates: dict[
        Literal[
            "description",
            "status",
            "source_url",
        ],
        str
    ]


class DocumentUpdateGenerator(ABC):
    """Base class for document updates."""
    @abstractmethod
    def update_source(self) -> Generator[DocumentUpdate, None, None]:
        """Generate document updates for processing from the configured source"""

        raise NotImplementedError("update_source() not implemented")


class HandleUploadResult(BaseModel):
    """Result of handling a document update."""

    document_update: DocumentUpdate
    error: Optional[str] = None
