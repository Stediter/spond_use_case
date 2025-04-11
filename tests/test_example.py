from pyspark.testing import assertDataFrameEqual
from pyspark.sql.types import *

from modules.data_quality import check_data_quality_id


def test_check_data_quality_id(test_spark):
    test_data = [("abc",1),
                 (None,1),
                 ("cde",1),
                 ("cde",1)]
    test_schema = StructType([StructField("test_id_column",StringType(),True),
                              StructField("test_other_column",IntegerType(),True)])
    test_df = test_spark.createDataFrame(data=test_data,
                                         schema=test_schema)
    expected_good_df = test_df.where("test_id_column = 'abc'")
    expected_bad_df = test_df.where("test_id_column <> 'abc' OR test_id_column IS NULL")

    actual_good_df, actual_bad_df = check_data_quality_id(test_df,"test_id_column")

    assertDataFrameEqual(actual_good_df,expected_good_df)
    assertDataFrameEqual(actual_bad_df,expected_bad_df)