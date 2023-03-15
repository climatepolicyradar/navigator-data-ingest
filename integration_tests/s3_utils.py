from botocore.exceptions import ClientError
from cloudpathlib import S3Path
import json


def remove_bucket(s3, bucket_name: str) -> None:
    """Remove an S3 bucket."""
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))
        raise Exception("Bucket Does Not Exist")


def remove_objects(s3, bucket_name: str) -> None:
    """Remove all the contents of an s3 bucket."""
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
    except ClientError as e:
        print("The bucket does not exist: {}".format(e))
        raise Exception("Bucket Does Not Exist")


def build_bucket(s3, bucket_name: str, location: dict) -> None:
    """Build an S3 bucket."""
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


def upload_file_to_bucket(
    s3, bucket_name: str, upload_path: str, local_file_path: str
) -> None:
    """Upload a file to an S3 bucket."""
    s3.upload_file(local_file_path, bucket_name, upload_path)


def create_dictionary_from_s3_bucket(
    bucket_path: S3Path, name_key: str, glob_pattern: str
) -> dict[dict]:
    """Create a dictionary from all the json files in an S3 bucket using the specified name_key to access the data."""
    json_files = bucket_path.glob(glob_pattern)
    data = {}
    for json_file in json_files:
        file_data = json.loads(json_file.read_text())
        data[file_data[name_key]] = file_data
    return data


def read_local_s3_json_file(file_path: str) -> dict[dict]:
    """Read a local json file and return the data."""
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data
