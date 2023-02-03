import json

from cloudpathlib import S3Path

from navigator_data_ingest.base.types import InputData


def read_s3_json_file(input_file: S3Path) -> dict:
    """Read a JSON file contents from S3."""
    with input_file.open("r") as input_data:
        return json.load(input_data)


def get_input_data(input_file: S3Path) -> InputData:
    """Read a JSON file contents from S3."""
    json_data = read_s3_json_file(input_file)

    try:
        input_data = InputData(
            new_documents=json_data["new_documents"],
            updated_documents=json_data["updated_documents"],
        )

    except KeyError as e:
        raise ValueError(f"Input data missing required key: {e}")

    return input_data
