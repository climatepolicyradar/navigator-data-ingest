import os
import json
import textwrap
import traceback
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator
from pathlib import Path

import boto3
from click.testing import CliRunner
import pytest
from moto import mock_aws

from navigator_data_ingest.main import main

FIXTURE_DATA_DIR = Path(__file__).parent / "fixtures"


class S3BucketFactory:
    """Factory for creating moto-stubbed S3 buckets."""

    def __init__(self, s3_client):
        self.s3_client = s3_client

    def create_bucket(self, name: str, files: Optional[Dict[str, Any]] = None) -> str:
        """Create a bucket with optional files."""
        self.s3_client.create_bucket(
            Bucket=name, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
        )

        if files:
            for key, content in files.items():
                if isinstance(content, dict):
                    content = json.dumps(content)
                if isinstance(content, str):
                    content = content.encode("utf-8")

                self.s3_client.put_object(Bucket=name, Key=key, Body=content)

        return name

    def list_bucket_file_names(
        self, bucket_name: str, prefix: Optional[str] = None
    ) -> list:
        """List all files in a bucket, optionally filtered by prefix."""
        kwargs = {"Bucket": bucket_name}
        if prefix:
            kwargs["Prefix"] = prefix

        objects = self.s3_client.list_objects_v2(**kwargs)
        return [obj["Key"] for obj in objects.get("Contents", [])]

    def get_file(self, bucket_name: str, path: str) -> Any:
        """Return the contents of a file or None if it doesn't exist."""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=path)
        except self.s3_client.exceptions.NoSuchKey:
            return None

        content = response["Body"].read()
        return content


@pytest.fixture
def s3_mock_factory() -> Generator[S3BucketFactory, None, None]:
    """Factory for creating S3 buckets with moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"

    with mock_aws():
        # Configure cloudpathlib to use moto
        s3_client = boto3.client("s3", region_name="eu-west-1")

        yield S3BucketFactory(s3_client)


@pytest.fixture(autouse=True)
def thread_executor(monkeypatch):
    """Force the CLI to use a thread pool executor for compatibility with moto."""

    monkeypatch.setattr(
        "navigator_data_ingest.main.ProcessPoolExecutor", ThreadPoolExecutor
    )
    yield


@pytest.fixture
def mock_pdf_downloads(requests_mock):
    pdf_bytes = (FIXTURE_DATA_DIR / "sample.pdf").read_bytes()
    pdf_url = "https://climatepolicyradar.org/file.pdf"
    requests_mock.get(
        pdf_url,
        content=pdf_bytes,
        headers={"Content-Type": "application/pdf"},
    )

    return pdf_url


@pytest.fixture
def mock_html_downloads(requests_mock):
    html_url = "https://climatepolicyradar.org/page.html"
    requests_mock.get(
        html_url,
        text=textwrap.dedent(
            f"""
            <html>
            <head><title>Mocked content</title></head>
            <body>
                <h1>Mocked content for {html_url}</h1>
                <p>This content is provided by the integration test fixture.</p>
            </body>
            </html>
            """
        ),
        headers={"Content-Type": "text/html; charset=utf-8"},
    )

    return html_url


def parse_runner_result(result):
    exc_info = str(result.exc_info)
    if result.exception:
        exc_info = "".join(
            traceback.format_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )
        )
    error_msg = textwrap.dedent(
        f"""
        CLI command failed with exit code {result.exit_code}

        Output:
        {result.output or 'None'}

        Exception info:
        {exc_info}
        """
    )

    return error_msg


def load_test_data_from_dir(directory) -> dict[str, Any]:
    """Load test data from integration test fixtures."""
    fixture_dir = FIXTURE_DATA_DIR / directory
    assert fixture_dir.exists()
    result = {}

    # Recursively walk through all files in the fixture directory
    for file_path in fixture_dir.rglob("*"):
        if file_path.is_file():
            # Get relative path from fixture_dir
            relative_path = file_path.relative_to(fixture_dir)
            key = str(relative_path)

            # Handle different file types
            if file_path.suffix == ".json":
                with open(file_path, "r") as f:
                    result[key] = json.load(f)
            elif file_path.suffix == ".npy":
                # For .npy files, we'll store the raw bytes since they're binary
                with open(file_path, "rb") as f:
                    result[key] = f.read()
            else:
                # For other file types, read as text
                with open(file_path, "r") as f:
                    result[key] = f.read()

    return result


def test_integration__no_op(s3_mock_factory):
    """Run with no document actions."""

    # Create mock buckets
    pipeline_files = {
        "input/2022-11-01T21.53.26.945831/new_and_updated_documents.json": {
            "new_documents": [],
            "updated_documents": {},
        },
        "input/2022-11-01T21.53.26.945831/db_state.json": {},
    }
    pipeline_bucket = s3_mock_factory.create_bucket(
        "test-pipeline-bucket", pipeline_files
    )
    document_bucket = s3_mock_factory.create_bucket("test-document-bucket")

    # Run
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--pipeline-bucket",
            pipeline_bucket,
            "--document-bucket",
            document_bucket,
            "--updates-file-name",
            "new_and_updated_documents.json",
            "--output-prefix",
            "parser_input",
            "--embeddings-input-prefix",
            "embeddings_input",
            "--indexer-input-prefix",
            "indexer_input",
            "--db-state-file-key",
            "input/2022-11-01T21.53.26.945831/db_state.json",
        ],
    )
    assert result.exit_code == 0, parse_runner_result(result)

    # Confirm no-op
    original_files = set(pipeline_files.keys())
    after_files = set(s3_mock_factory.list_bucket_file_names(pipeline_bucket))
    assert original_files == after_files


def test_integration__with_files(
    s3_mock_factory,
    mock_pdf_downloads,
    mock_html_downloads,
):
    """Run with many new & update document actions."""

    # Create mock buckets
    pipeline_files = load_test_data_from_dir("pipeline_in")
    pipeline_bucket = s3_mock_factory.create_bucket(
        "test-pipeline-bucket", pipeline_files
    )
    document_bucket = s3_mock_factory.create_bucket("test-document-bucket")

    # Run
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--pipeline-bucket",
            pipeline_bucket,
            "--document-bucket",
            document_bucket,
            "--updates-file-name",
            "new_and_updated_documents.json",
            "--output-prefix",
            "parser_input",
            "--embeddings-input-prefix",
            "embeddings_input",
            "--indexer-input-prefix",
            "indexer_input",
            "--db-state-file-key",
            "input/2022-11-01T21.53.26.945831/db_state.json",
        ],
    )

    assert result.exit_code == 0, parse_runner_result(result)

    # Legacy test cases
    # Ported from the original "online" tests that ran against third parties

    # test_pipeline_bucket_json_errors
    # Legacy test saught to ensure no error file was written to the bucket
    pipeline_files = s3_mock_factory.list_bucket_file_names(pipeline_bucket)
    err_files = [i for i in pipeline_files if i.endswith(".json_errors")]
    assert (
        len(err_files) == 0
    ), f"{err_files=}: {s3_mock_factory.get_file(pipeline_bucket, err_files[0])}"

    # test_pipeline_bucket_files
    # Legacy test was a file count, the port is more specific
    assert len(s3_mock_factory.list_bucket_file_names(pipeline_bucket, "input/")) == 2
    assert (
        len(s3_mock_factory.list_bucket_file_names(pipeline_bucket, "archive/")) == 15
    )
    assert (
        len(
            s3_mock_factory.list_bucket_file_names(pipeline_bucket, "embeddings_input/")
        )
        == 3
    )
    assert (
        len(s3_mock_factory.list_bucket_file_names(pipeline_bucket, "parser_input/"))
        == 22
    )
    assert (
        len(s3_mock_factory.list_bucket_file_names(pipeline_bucket, "indexer_input/"))
        == 0
    )

    # test_pipeline_bucket_json
    # Legacy test compared metadata in json files
    issues = []
    for key in pipeline_files:
        if key.startswith("input/"):
            continue
        if not key.endswith(".json"):
            continue
        if "archive/" in key:
            continue

        bucket_file_content = json.loads(s3_mock_factory.get_file(pipeline_bucket, key))
        local_path = FIXTURE_DATA_DIR / "pipeline_out" / key
        with open(local_path) as f:
            local_file_content = json.load(f)

        # Fields that should be set but we're not comparing
        fields_we_dont_care_about_matching = [
            "document_md5_sum",  # Non deterministic
            "document_cdn_object",  # Non deterministic
        ]

        # Other fields we expect a match
        for (bucket_field, bucket_value), (local_field, local_value) in zip(
            bucket_file_content.items(), local_file_content.items()
        ):
            assert (
                bucket_field == local_field
            )  # preventing a regression on field ordering
            if bucket_field in fields_we_dont_care_about_matching:
                continue
            if bucket_value != local_value:
                issues.append(
                    f"{key} didnt match local version for {bucket_field}: {bucket_value=} & {local_value=}"
                )

    assert not issues, f"Found issues in {len(issues)} files: {issues}"

    # test_pipeline_bucket_npy
    # Legacy test was supposed to check the state of the npy files matched the local
    # comparison, but it actually did this by iterating the bucket files and asserting
    # they also existed locally, the local comparison has extra then what we expect so
    # in porting this its been made more specific
    pipeline_files = s3_mock_factory.list_bucket_file_names(pipeline_bucket)
    bucket_files_npy = sorted([i for i in pipeline_files if i.endswith(".npy")])
    assert len(bucket_files_npy) == 2
    assert bucket_files_npy[0].startswith(
        "archive/indexer_input/TESTCCLW.executive.1.1/"
    )
    assert bucket_files_npy[1].startswith(
        "archive/indexer_input/TESTCCLW.executive.2.2/"
    )

    # Extra test on top of the legacy tests confirming some documents actually downloaded
    document_files = s3_mock_factory.list_bucket_file_names(document_bucket)
    assert len(document_files) > 0
