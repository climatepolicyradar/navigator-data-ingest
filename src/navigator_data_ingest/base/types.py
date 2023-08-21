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
    Callable,
)

from cpr_data_access.parser_models import ParserInput
from pydantic import BaseModel

from cpr_data_access.pipeline_general_models import (
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    CONTENT_TYPE_DOCX,
    UpdateTypes,
    BackendDocument,
    Update,
)

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


PipelineFieldMapping = {
    UpdateTypes.NAME: "document_name",
    UpdateTypes.DESCRIPTION: "document_description",
    UpdateTypes.SOURCE_URL: "document_source_url",
    UpdateTypes.PUBLICATION_TS: "publication_ts",
}


class UploadResult(BaseModel):
    """Information generated during the upload of a document used by later processes"""

    cdn_object: Optional[str]
    md5_sum: Optional[str]
    content_type: Optional[str]


class HandleResult(BaseModel):
    """Result of handling an input file"""

    parser_input: ParserInput
    error: Optional[str] = None


class UnsupportedContentTypeError(Exception):
    """An error indicating a content type not yet supported by parsing"""

    def __init__(self, content_type: str):
        self.content_type = content_type
        super().__init__(f"Content type '{content_type}' is not supported for caching")


class UpdateResult(BaseModel):
    """Result of updating a document update via the ingest stage."""

    document_id: str
    update: Update
    error: Optional[str] = None


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
    def process_new_documents(self) -> Generator[BackendDocument, None, None]:
        """Generate new documents for processing from the configured source"""

        raise NotImplementedError("process_new_documents() not implemented")

    @abstractmethod
    def process_updated_documents(self) -> Generator[Update, None, None]:
        """Generate documents with updates for processing from the configured source"""

        raise NotImplementedError("process_updated_documents() not implemented")


class Action(BaseModel):
    """Base class for associating an update with the relevant action."""

    update: Update
    action: Callable
