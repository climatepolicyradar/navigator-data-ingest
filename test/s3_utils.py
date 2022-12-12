from botocore.exceptions import ClientError


def remove_bucket(s3, bucket_name: str) -> None:
    """
    Remove an S3 bucket.
    """
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))


def remove_objects(s3, bucket_name: str) -> None:
    """
    Remove all the contents of an s3 bucket.
    """
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))


def build_bucket(s3, bucket_name: str, location: dict) -> None:
    """
    Build an S3 bucket.
    """
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


def upload_file_to_bucket(s3, bucket_name: str, upload_path: str, local_file_path: str) -> None:
    """
    Upload a file to an S3 bucket.
    """
    s3.upload_file(local_file_path, bucket_name, upload_path)





