from cloudpathlib import S3Path
import json

bucket_path = S3Path("s3://ingest-unit-test-pipeline-bucket/ingest_unit_test_parser_input/")

json_files = bucket_path.glob("*.json")

json_data = []
for json_file in json_files:
    data = json.loads(json_file.read_text())
    json_data.append(data)

def test_dummy():
    assert True
