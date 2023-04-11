import os
from pathlib import Path
import json
from cloudpathlib import S3Path
import pytest


def get_local_dir_files(local_dir: str, suffix: str) -> list[Path]:
    """Get all the files in a local directory."""
    return list(
        Path(
            Path(__file__).parent / os.path.join("data", "pipeline_out", local_dir)
        ).glob(f"*{suffix}")
    )


def get_local_fp(file: S3Path) -> Path:
    """Get the local file path."""
    return Path(Path(__file__).parent / os.path.join("data", "pipeline_out", file.key))


def timestamped_file(file: S3Path) -> bool:
    """Check if a file is timestamped."""
    return file.name.startswith("20")


@pytest.mark.integration
def test_pipeline_bucket_files(
    bucket_files_npy, bucket_files_json, bucket_files_json_errors
):
    """Test that all the files we expect to exist in the bucket do exist after running the ingest stage."""
    p = Path(Path(__file__).parent / os.path.join("data", "pipeline_out")).glob("**/*")
    local_files = [x for x in p if x.is_file()]

    bucket_files = bucket_files_json + bucket_files_npy + bucket_files_json_errors

    assert len(local_files) == len(bucket_files)


@pytest.mark.integration
def test_pipeline_bucket_json(bucket_files_json):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    for file in bucket_files_json:
        s3_data = json.loads(file.read_text())
        if timestamped_file(file):
            local_data = json.loads(
                get_local_dir_files(
                    local_dir=file.key.replace(file.name, ""), suffix=file.suffix
                )[0].read_text()
            )
        else:
            local_data = json.loads(get_local_fp(file).read_text())

        assert s3_data == local_data


@pytest.mark.integration
def test_pipeline_bucket_npy(bucket_files_npy):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    for file in bucket_files_npy:
        if timestamped_file(file):
            local_file = get_local_dir_files(
                local_dir=file.key.replace(file.name, ""), suffix=file.suffix
            )[0]
        else:
            local_file = get_local_fp(file)
        assert local_file.exists()


@pytest.mark.integration
def test_pipeline_bucket_json_errors(bucket_files_json_errors):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    for file in bucket_files_json_errors:
        s3_data = json.loads(file.read_text())
        local_data = json.loads(get_local_fp(file).read_text())

        assert set([i.split(":")[0] for i in s3_data]) == set(
            [i.split(":")[0] for i in local_data]
        )
