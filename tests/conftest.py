import pytest
from pyspark.sql import SparkSession
@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("chicago_taxi") \
        .getOrCreate()
