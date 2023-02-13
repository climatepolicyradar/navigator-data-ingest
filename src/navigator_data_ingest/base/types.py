"""Base definitions for data ingest"""
import json
from abc import abstractmethod, ABC
from dataclasses import dataclass
import datetime
from enum import Enum
from typing import Any, Generator, Mapping, Optional, Sequence, Literal, Union, List
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
        str,
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


class PipelineStages(BaseModel):
    """Expected location of the pipeline stages in the s3 bucket."""

    parser_input: str
    embeddings_input: str
    indexer_input: str


@dataclass
class UpdateResult:
    """Class describing the results of comparing csv data against the db data to identify updates."""

    db_value: Union[str, datetime.datetime]
    csv_value: Union[str, datetime.datetime]
    updated: bool
    type: Literal["PhysicalDocument", "Family", "FamilyDocument"]
    field: str


class InputData(BaseModel):
    """Expected input data containing both document updates and new documents for the ingest stage of the pipeline."""

    new_documents: List[dict]
    updated_documents: dict[str, List[UpdateResult]]

    def to_json(self) -> dict:
        updated_documents_json = {}
        for update in self.updated_documents:
            for update_result in self.updated_documents[update]:
                updated_documents_json[update] = [
                    json.loads(json.dumps(update_result.__dict__))
                ]
        if "__pydantic_initialised__" in updated_documents_json.keys():
            updated_documents_json.pop("__pydantic_initialised__")

        return {
            "new_documents": self.new_documents,
            "updated_documents": updated_documents_json,
        }


class UpdateConfig(BaseModel):
    """Shared configuration for document update functions."""

    pipeline_bucket: str
    input_prefix: str
    pipeline_stage_prefixes: dict[str, str]
    archive_prefix: str


class DocumentGenerator(ABC):
    """Base class for all document sources."""

    @abstractmethod
    def process_new_documents(self) -> Generator[Document, None, None]:
        """Generate new documents for processing from the configured source"""

        raise NotImplementedError("process_new_documents() not implemented")

    @abstractmethod
    def process_updated_documents(self) -> Generator[UpdateResult, None, None]:
        """Generate documents with updates for processing from the configured source"""

        raise NotImplementedError("process_updated_documents() not implemented")
