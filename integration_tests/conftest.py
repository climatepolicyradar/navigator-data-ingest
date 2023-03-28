import pytest
from cloudpathlib import S3Path
import os


@pytest.fixture
def bucket_path():
    """Get the bucket path."""
    return S3Path(os.path.join("s3://", os.environ.get("INGEST_PIPELINE_BUCKET")))
