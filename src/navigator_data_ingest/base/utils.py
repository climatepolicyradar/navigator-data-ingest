import json

from cloudpathlib import S3Path


def read_s3_json_file(input_file: S3Path) -> dict:
    """Read a JSON file contents from S3."""
    with input_file.open("r") as input_data:
        return json.load(input_data)
