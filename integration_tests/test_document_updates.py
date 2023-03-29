"""Assert that the document updates made to existing documents are correctly processed correctly by the ingest stage."""
import pytest


@pytest.mark.integration
def test_update_1():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'name'

    Document ID:
    - TESTCCLW.executive.1.1

    Expected Result:
    - Document name should be updated in the json objects
    - The npy file should be removed from the indexer input prefix to trigger re-creation
    """

    assert True


@pytest.mark.integration
def test_update_2():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'description'

    Document ID:
    - TESTCCLW.executive.2.2

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_3():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'source_url'

    Document ID:
    - TESTCCLW.executive.3.3

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_4():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update:
    - Update to document 'description' and 'source_url'

    Document ID:
    - TESTCCLW.executive.6.6

    Expected Result:
    """
    assert True


@pytest.mark.integration
def test_update_5():
    """
    Assert that the json objects for the document in s3 are as expected post ingest stage run.

    Update: - Update to document 'description' (for this document it simulates a document that has is currently being
    processed i.e. doesn't exist in all the s3 prefixes).

    Document ID:
    - TESTCCLW.executive.7.7

    Expected Result:
    """
    assert True
