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
    return str(file.name).startswith("20")


@pytest.mark.integration
def test_pipeline_bucket_files(
    bucket_files_npy, bucket_files_json, bucket_files_json_errors
):
    """Test that all the files we expect to exist in the bucket do exist after running the ingest stage."""
    p = Path(Path(__file__).parent / os.path.join("data", "pipeline_out")).glob("**/*")

    local_files = [
        x
        for x in p
        if (x.is_file() and (x.name.endswith(".json") or x.name.endswith(".npy")))
    ]
    bucket_files = bucket_files_json + bucket_files_npy + bucket_files_json_errors

    assert len(local_files) == len(bucket_files)


@pytest.mark.integration
def test_pipeline_bucket_json(bucket_files_json):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    assert len(bucket_files_json) > 0
    all_s3_objects = []
    all_local_objects = []
    file_paths = []

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

        if (
            "input_dir_path" in s3_data.keys()
        ):  # skip execution_data file as the content changes each run (bucket name)
            continue

        # Remove md5sum and document_cdn_object from the data as these are not
        # deterministic across runs if the source data has changed.
        s3_md5sum = s3_data.get("document_md5_sum")
        if s3_md5sum:
            s3_data["document_md5_sum"] = "MD5SUM"
        s3_data["document_cdn_object"] = (
            s3_data["document_cdn_object"].replace(s3_md5sum, "MD5SUM")
            if s3_data.get("document_cdn_object")
            else None
        )

        local_md5sum = local_data.get("document_md5_sum")
        if local_md5sum:
            local_data["document_md5_sum"] = "MD5SUM"
        local_data["document_cdn_object"] = (
            local_data["document_cdn_object"].replace(local_md5sum, "MD5SUM")
            if local_data.get("document_cdn_object")
            else None
        )

        all_s3_objects.append(s3_data)
        all_local_objects.append(local_data)
        file_paths.append(str(file))

    differences = []
    for i, (s3, local, path) in enumerate(
        zip(all_s3_objects, all_local_objects, file_paths)
    ):
        if s3 != local:
            diff_keys = []
            all_keys = set(s3.keys()) | set(local.keys())
            for key in all_keys:
                if s3[key] != local[key]:
                    diff_keys.append(
                        {
                            "key": key,
                            "s3": s3[key],
                            "local": local[key],
                        }
                    )

            differences.append(f"File {path} (item #{i+1}): {diff_keys}")

    # Format detailed error message if differences exist
    assert not differences, f"Found differences in {len(differences)} files"


@pytest.mark.integration
def test_pipeline_bucket_npy(bucket_files_npy):
    """Test that the pipeline bucket is in the expected state after the ingest stage run."""
    assert len(bucket_files_npy) > 0
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
