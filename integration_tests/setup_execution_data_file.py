import json
import sys
import os
from pathlib import Path

from cloudpathlib import S3Path


def create_execution_data_file(bucket_name: str, execution_data_key: str, new_and_updated_documents_file_name: str) -> None:
    """Create the test execution data file dynamically from environment variables in the test."""
    data = {
        "input_dir_path": str(S3Path(f"s3://{bucket_name}/{new_and_updated_documents_file_name}").parent),
    }

    local_output_path = os.path.join(os.getcwd(), f'integration_tests/data/pipeline_in/{execution_data_key}')

    if Path(local_output_path).exists():
        os.remove(local_output_path)

    if not Path(local_output_path).parent.exists():
        os.mkdir(Path(local_output_path).parent)

    with open(local_output_path, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    create_execution_data_file(
        bucket_name=sys.argv[1],
        execution_data_key=sys.argv[2],
        new_and_updated_documents_file_name=sys.argv[3],
    )
