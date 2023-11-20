import pytest
from cloudpathlib import S3Path
import os


def get_bucket_files_with_suffix(bucket: S3Path, suffix: str) -> list[S3Path]:
    """Get all the files in a bucket with a given suffix."""
    bucket_files = []
    for pattern in ["*", "*/*", "*/*/*", "*/*/*/*"]:
        files = list(bucket.glob(pattern + suffix))
        bucket_files.extend(set(files))
    return bucket_files


@pytest.fixture
def bucket_path():
    """Get the bucket path."""
    return S3Path(os.path.join("s3://", str(os.environ.get("INGEST_PIPELINE_BUCKET"))))


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
