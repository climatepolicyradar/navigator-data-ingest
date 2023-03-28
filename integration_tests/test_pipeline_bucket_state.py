import json
from pathlib import Path

from cloudpathlib import S3Path


# TODO assert that the documents are uploaded to the document bucket

# TODO assert that the final start of the pipeline bucket is as expected - normalize all the timestamped file names -
#  assert that the files are as expected by getting the local relative file path and using that as the key to get the
#  expected data


def get_local_dir_files(local_dir: str, suffix: str) -> list[Path]:
    """Get all the files in a local directory."""
    return list(Path(local_dir).glob(f"*/*{suffix}"))


def timestamped_file(file: S3Path) -> bool:
    """Check if a file is timestamped."""
    return file.name.startswith("20")


def test_pipeline_bucket(pipeline_bucket_files):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    for file in pipeline_bucket_files:
        s3_data = json.loads(file.read_text())
        if timestamped_file(file):
            local_data = json.loads(
                get_local_dir_files(local_dir=str(file.parent), suffix=file.suffix)[
                    0
                ].read_text()
            )
        else:
            local_data = json.loads(Path(file.key).read_text())
        assert s3_data == local_data
