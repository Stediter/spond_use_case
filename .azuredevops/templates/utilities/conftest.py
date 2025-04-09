import datetime
import pytest


@pytest.fixture(scope="session")
def test_spark():

    if "spark" in globals():
        test_spark = globals["spark"]
    else:
        try:
            from databricks.connect import DatabricksSession
            test_spark = DatabricksSession.builder.getOrCreate()

        except ImportError:
            from pyspark.sql import SparkSession
            test_spark = SparkSession.builder.getOrCreate()
    
    return test_spark


@pytest.fixture(scope="session")
def test_dbutils():

    if "dbutils" in globals():
            test_session = globals["dbutils"]
    else:
        try:
            from databricks.sdk.runtime import dbutils as sdk_dbutils
            test_dbutils = sdk_dbutils

        except ImportError:
            from pyspark.dbutils import DBUtils
            test_dbutils = DBUtils()
    
    return test_dbutils
