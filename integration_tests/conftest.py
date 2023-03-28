import pytest
from cloudpathlib import S3Path
import os


PIPELINE_BUCKET = S3Path(
    os.path.join("s3://", os.environ.get("INGEST_PIPELINE_BUCKET"))
)


@pytest.fixture
def pipeline_bucket_files() -> list[S3Path]:
    """Return a list of all files in the pipeline bucket."""
    pipeline_files = []
    for pattern in ["*.*", "*/*.*", "*/*/*.*", "*/*/*/*.*"]:
        files = list(PIPELINE_BUCKET.glob(pattern))
        pipeline_files.extend(files)
    return pipeline_files
