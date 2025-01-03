# Navigator Data Ingest

## How to Update Test Data in the Integration Tests

The test procedure follows the deploy-test-destroy pattern.

This involves creating a test environment, running the tests, and then destroying the test environment.
- docker image built
- document and pipeline buckets are created
- test data is uploaded to the pipeline bucket
- docker container is run
- test data is downloaded from the pipeline bucket and compared against the expected output
  - asserted that all the files that are expected and declared locally exist in s3
  - asserted that the content of the files in s3 is as expected
- s3 bucket objects and the bucket itself are removed

The integration tests strongly assert the output against the expected output. Should the ingest stage functionality change then the expected output can be updated by the following process.

Effectively follow the first 3 steps of the integration-tests.yml github actions flow, assert the output is correct and then sync to bucket to the pipeline_out directory.

[//]: # (TODO: Build these steps into a bash script or something)

### Build the docker image locally

```shell
     make build
```

### MAKE SURE YOU HAVE THE CORRECT AWS CREDENTIALS SET UP

```shell
    export AWS_PROFILE=${PROFILE_NAME}
```

Example:

```shell
    export AWS_PROFILE=sandbox
```

### Set up the test buckets

```shell
     python -m integration_tests.setup_test_buckets ${document_bucket} ${pipeline_bucket} ${region}
```

Example:

```shell
     python -m integration_tests.setup_test_buckets docbucket123123123 pipbucket123123123 eu-west-1
```

### Sync the test data to the s3 bucket

```shell
     aws s3 sync integration_tests/data/pipeline_in s3://${pipeline_bucket}
```

Example:

```shell
     aws s3 sync integration_tests/data/pipeline_in s3://pipbucket123123123
```

### Run the docker image

If you are trying to figure out what the variables are look in the env var section of the following file: .github/workflows/integration-tests.yml. Also note that the prefixes used must match the subdirectory names of the data/pipeline_in directory.

```shell
    docker run -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -e API_HOST="" -e MACHINE_USER_EMAIL="" -e MACHINE_USER_PASSWORD="" navigator-data-ingest --pipeline-bucket ${PIPELINE_BUCKET} --document-bucket ${DOCUMENT_BUCKET} --updates-file-name ${TEST_DATA_UPLOAD_PATH} --output-prefix ${OUTPUT_PREFIX} --embeddings-input-prefix ${EMBEDDINGS_INPUT_PREFIX} --indexer-input-prefix ${INDEXER_INPUT_PREFIX} --db-state-file-key ${DB_STATE_FILE_KEY}
```

Example:

```shell
    docker run -e AWS_ACCESS_KEY_ID=XXX -e AWS_SECRET_ACCESS_KEY=XXX -e API_HOST="" -e MACHINE_USER_EMAIL="" -e MACHINE_USER_PASSWORD="" navigator-data-ingest --pipeline-bucket pipbucket123123123 --document-bucket docbucket123123123 --updates-file-name new_and_updated_documents.json --output-prefix ingest_unit_test_parser_input --embeddings-input-prefix ingest_unit_test_embeddings_input --indexer-input-prefix ingest_unit_test_indexer_input --db-state-file-key input/2022-11-01T21.53.26.945831/db_state.json
```

### Sync Down Output

Assert that the output is correct and if so manually delete all the files in the pipeline_out directory and sync the data locally to the pipeline_out directory

```shell
     cd integration_tests/data/pipeline_out
     
     rm -rf *

     aws s3 sync s3://${pipeline_bucket}/ .
```

### Remove the test buckets

```shell
     python -m integration_tests.remove_test_buckets ${document_bucket} ${pipeline_bucket} ${region}
```

Example:

```shell
     python -m integration_tests.remove_test_buckets docbucket123123123 pipbucket123123123 eu-west-1
```

### Format the output jsons (optional)

This is useful if you want to easily be able to read the jsons and compare them via a PR.

I would recommend committing any changes, running the reformat program and carefully checking that there are no errors and that the data looks correct.

```shell
     python -m format_jsons
```
