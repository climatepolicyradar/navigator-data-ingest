import json
from typing import Generator, List

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import (
    Document,
    DocumentGenerator,
    UpdateResult,
)


class LawPolicyGenerator(DocumentGenerator):
    """A generator of validated Document objects for inspection & upload"""

    def __init__(self, input_file: S3Path):
        json_data = read_s3_json_file(input_file)
        self.new_documents = json_data["new_documents"]
        self.updated_documents = json_data["updated_documents"]

    def process_new_documents(self) -> Generator[Document, None, None]:
        """Generate documents for processing from the configured source."""
        for d in self.new_documents:
            yield Document(**d)

    def process_updated_documents(
        self,
    ) -> Generator[dict[str, List[UpdateResult]], None, None]:
        """Generate documents for updating in s3 from the configured source."""
        for doc_id, update in self.updated_documents.items():
            try:
                yield {
                    doc_id,
                    [
                        UpdateResult(
                            db_value=update_result["db_value"],
                            csv_value=update_result["csv_value"],
                            updated=update_result["updated"],
                            type=update_result["type"],
                            field=update_result["field"],
                        )
                        for update_result in update
                    ],
                }
            except KeyError as e:
                raise ValueError(f"Input data missing required key: {e}")


def read_s3_json_file(input_file: S3Path) -> dict:
    """Read a JSON file contents from S3."""
    with input_file.open("r") as input_data:
        return json.load(input_data)
