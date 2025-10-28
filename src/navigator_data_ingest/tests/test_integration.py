import os
import json
import textwrap
import traceback
from typing import Dict, Any

from click.testing import CliRunner
import pytest
from moto import mock_aws
import boto3

from navigator_data_ingest.main import main


class S3BucketFactory:
    """Factory for creating moto-stubbed S3 buckets."""
    
    def __init__(self, s3_client):
        self.s3_client = s3_client
    
    def create_bucket(self, name: str, files: Dict[str, Any] = None) -> str:
        """Create a bucket with optional files."""
        self.s3_client.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
        )
        
        if files:
            for key, content in files.items():
                if isinstance(content, dict):
                    content = json.dumps(content)
                if isinstance(content, str):
                    content = content.encode('utf-8')
                
                self.s3_client.put_object(Bucket=name, Key=key, Body=content)
        
        return name
    


@pytest.fixture
def s3_mock_factory() -> S3BucketFactory:
    """Factory for creating S3 buckets with moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    
    with mock_aws():
        # Configure cloudpathlib to use moto
        from cloudpathlib import S3Path
        s3_client = boto3.client("s3", region_name="eu-west-1")
        
        # Set the client for cloudpathlib
        S3Path._cloud_meta.client = s3_client
        
        yield S3BucketFactory(s3_client)
    


def parse_runner_result(result):
    exc_info = str(result.exc_info)
    if result.exception:
        exc_info = "".join(
            traceback.format_exception(
                type(result.exception),
                result.exception,
                result.exception.__traceback__
            )
        )
    error_msg = textwrap.dedent(f"""
        CLI command failed with exit code {result.exit_code}

        Output:
        {result.output or 'None'}

        Exception info:
        {exc_info}
        """
    )

    return error_msg


def test_integration(s3_mock_factory):
    """Test CLI integration with mocked AWS credentials and S3 buckets."""
    runner = CliRunner()
    
    # Create pipeline bucket with test files
    pipeline_bucket = s3_mock_factory.create_bucket("test-pipeline-bucket", {
        "input/2022-11-01T21.53.26.945831/new_and_updated_documents.json": {
            "new_documents": [],
            "updated_documents": {}
        },
        "input/2022-11-01T21.53.26.945831/db_state.json": {}
    })
    
    # Create empty document bucket
    document_bucket = s3_mock_factory.create_bucket("test-document-bucket")
    
    result = runner.invoke(main, [
        '--pipeline-bucket', pipeline_bucket,
        '--document-bucket', document_bucket,
        '--updates-file-name', 'new_and_updated_documents.json',
        '--output-prefix', 'parser_input',
        '--embeddings-input-prefix', 'embeddings_input',
        '--indexer-input-prefix', 'indexer_input',
        '--db-state-file-key', 'input/2022-11-01T21.53.26.945831/db_state.json'
    ])
    
    assert result.exit_code == 0, parse_runner_result(result)

