# Navigator Data Ingest 

## Introduction 

This application forms the Data Ingest portion of the data processing pipeline.

The application reads json files from an input s3 directory and outputs the processed json objects as json files to an output s3 directory. 

Declarations of the input and output object types can be found in src.navigator_data_ingest.base.types 
- Input Object: Document
- Output Object: DocumentParserInput 

The main feature of the ingest stage is to download the source document, identify the content-type and upload the result to the relevant s3 document store if pdf. 


## Configuration 

The tool is a command line tool that can be run with the following command:

    python -m navigator_data_ingest

The following options exist for configuring the tool at runtime: 

S3 bucket name from which to read/write input/output files

    --pipeline-bucket

S3 bucket name in which to store cached documents
    
    --document-bucket

Location of JSON Document array input file
    
    --input-file

Prefix to apply to output files
    
    --output-prefix

Number of workers downloading/uploading cached documents

    --worker-count


## Unit Tests

The unit tests use a mock s3 client to simulate the s3 interactions. All the update functions are then tested for functionality. 


## Integration Tests 

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

Build the docker image locally

     make build_test

Set up the test buckets 

     python setup_test_buckets ${document_bucket} ${pipeline_bucket} ${region}

Sync the test data to the s3 bucket 

     aws s3 sync integration_tests/data/pipeline_in s3://${pipeline_bucket}

Run the docker image 

     docker run -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -e API_HOST="" -e MACHINE_USER_EMAIL="" -e MACHINE_USER_PASSWORD="" navigator-data-ingest-test --pipeline-bucket ${PIPELINE_BUCKET} --document-bucket ${DOCUMENT_BUCKET} --input-file ${TEST_DATA_UPLOAD_PATH} --output-prefix ${OUTPUT_PREFIX} --embeddings-input-prefix ${EMBEDDINGS_INPUT_PREFIX} --indexer-input-prefix ${INDEXER_INPUT_PREFIX}

Assert that the output is correct and if so snyc the data locally to the pipeline_out directory 

     cd integration_tests/data/pipeline_out

     aws s3 sync s3://${pipeline_bucket}/ .