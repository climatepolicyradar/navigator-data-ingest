import boto3


def remove_bucket(s3, bucket_name: str) -> None:
    """
    Remove an S3 bucket and all of its contents.
    """
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    bucket.delete()


def build_bucket(s3, bucket_name: str) -> None:
    """
    Build an S3 bucket.
    """
    bucket = s3.Bucket(bucket_name)
    bucket.create()
    bucket.wait_until_exists()


def upload_file_to_bucket(s3, bucket_name: str, upload_path: str) -> None:
    """
    Upload a file to an S3 bucket.
    """
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(upload_path)


def setup_test_data(document_bucket_name: str, pipeline_bucket_name: str, test_data_file_path: str) -> None:
    """
    Setup test data for the integration tests.
    """
    s3 = boto3.resource('s3')

    s3.remove_bucket(document_bucket_name)
    s3.remove_bucket(pipeline_bucket_name)

    s3.build_bucket(document_bucket_name)
    s3.build_bucket(pipeline_bucket_name)

    s3.upload_file_to_bucket(pipeline_bucket_name, test_data_file_path)


def teardown_test_data(document_bucket_name: str, pipeline_bucket_name: str) -> None:
    """
    Teardown test data for the integration tests.
    """
    s3 = boto3.resource('s3')

    s3.remove_bucket(document_bucket_name)
    s3.remove_bucket(pipeline_bucket_name)
