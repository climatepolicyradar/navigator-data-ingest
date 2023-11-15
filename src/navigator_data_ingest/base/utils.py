import json
import logging
from typing import Generator, List, Tuple
from typing import cast

from cloudpathlib import CloudPath, S3Path

from navigator_data_ingest.base.types import DocumentGenerator
from cpr_data_access.pipeline_general_models import (
    Update,
    PipelineUpdates,
    BackendDocument,
)

_LOGGER = logging.getLogger(__file__)


class LawPolicyGenerator(DocumentGenerator):
    """
    A generator of:

    - Validated new Document objects for inspection & upload.
    - Validated Update objects for updating existing documents.
    """

    def __init__(self, input_file: S3Path, output_location_path: S3Path):
        """Initialize the generator."""
        _LOGGER.info("Initializing LawPolicyGenerator")
        json_data = read_s3_json_file(input_file)
        self.input_data = PipelineUpdates.model_validate(json_data)
        self.output_location_path = output_location_path

    def process_new_documents(self) -> Generator[BackendDocument, None, None]:
        """Generate documents for processing from the configured source."""
        _LOGGER.info("Processing new documents")
        for document in self.input_data.new_documents:
            if not parser_input_already_exists(self.output_location_path, document):
                yield document

    def process_updated_documents(
        self,
    ) -> Generator[Tuple[str, List[Update]], None, None]:
        """Generate documents for updating in s3 from the configured source."""
        _LOGGER.info("Processing updated documents")
        for document_id, document_updates in self.input_data.updated_documents.items():
            try:
                yield (
                    document_id,
                    document_updates,
                )
            except KeyError as e:
                raise ValueError(f"Input data missing required key: {e}")


def read_s3_json_file(input_file: S3Path) -> dict:
    """Read a JSON file contents from S3."""
    _LOGGER.info(
        "Reading input file.", extra={"props": {"input_file": str(input_file)}}
    )
    with input_file.open("r") as input_data:
        return json.load(input_data)


def parser_input_already_exists(
    output_location: CloudPath,
    document: BackendDocument,
) -> bool:
    """Check if the parser input file already exists."""
    output_file_location = cast(
        S3Path,
        output_location / f"{document.import_id}.json",
    )
    if output_file_location.exists():
        _LOGGER.info(
            f"Parser input for document ID '{document.import_id}' already exists"
        )
        return True
    return False
