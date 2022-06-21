import unittest
import sys
sys.path.insert(0, '../etl')
from etl import transform_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    def dataframes(self):

      input_schema = StructType([
          StructField('StoreID', IntegerType(), True),
          StructField('Location', StringType(), True),
          StructField('Date', StringType(), True),
          StructField('ItemCount', LongType(), True)
      ])

      input_df=self.spark.read.format("csv").schema(input_schema).load("input_data.csv")

      expected_schema = StructType([
          StructField('Location', StringType(), True),
          StructField('TotalItemCount', LongType(), True)
      ])

      expected_df=self.spark.read.format("csv").schema(expected_schema).load("expected_data.csv")

      return input_df,expected_df


    def test_etl(self):

        input_df,expected_df=self.dataframes()
        #Apply transforamtion on the input data frame
        transformed_df = transform_data(input_df)
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        # assert Test case 1
        self.assertTrue(res)


    def test_etl2(self):

      input_df,expected_df=self.dataframes()
      #Apply transforamtion on the input data frame
      transformed_df = transform_data(input_df)

      # Compare data in transformed_df and expected_df Test case 2
      self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

if __name__ == '__main__':
    unittest.main()