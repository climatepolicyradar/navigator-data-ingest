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

