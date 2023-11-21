import pytest
from cloudpathlib import S3Path
import os
import boto3

PIPELINE_BUCKET = os.environ.get("INGEST_PIPELINE_BUCKET")
S3_CLIENT = boto3.client("s3")


def get_bucket_files_with_suffix(bucket: str, suffix: str) -> list[S3Path]:
    """Retrieve all the files in an s3 bucket with a given suffix."""
    response = S3_CLIENT.list_objects_v2(Bucket=bucket)

    if "Contents" in response:
        files_with_suffix = [
            obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(suffix)
        ]

        # If there are more than 1000 objects continue listing
        while response["IsTruncated"]:
            response = S3_CLIENT.list_objects_v2(
                Bucket=bucket, ContinuationToken=response["NextContinuationToken"]
            )
            files_with_suffix.extend(
                [
                    obj["Key"]
                    for obj in response["Contents"]
                    if obj["Key"].endswith(suffix)
                ]
            )

        # Convert to s3 paths and return
        return [
            S3Path(os.path.join("s3://", bucket, file)) for file in files_with_suffix
        ]
    return []


@pytest.fixture
def bucket_path():
    """Get the bucket path."""
    if isinstance(PIPELINE_BUCKET, str) and len(PIPELINE_BUCKET) > 0:
        return PIPELINE_BUCKET
    raise ValueError(f"Invalid env var for PIPELINE_BUCKET: {str(PIPELINE_BUCKET)}")


@pytest.fixture
def bucket_files_json(bucket_path):
    """Get the bucket files with the .json suffix."""
    return get_bucket_files_with_suffix(bucket=bucket_path, suffix=".json")


@pytest.fixture
def bucket_files_npy(bucket_path):
    """Get the bucket files with the .npy suffix."""
    return get_bucket_files_with_suffix(bucket=bucket_path, suffix=".npy")


@pytest.fixture
def bucket_files_json_errors(bucket_path):
    """Get the bucket files with the .json_errors suffix."""
    return get_bucket_files_with_suffix(bucket=bucket_path, suffix=".json_errors")
