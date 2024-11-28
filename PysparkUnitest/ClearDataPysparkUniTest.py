from datetime import date, datetime

from marshmallow.utils import timestamp
from parameterized import parameterized
from sqlalchemy import Integer

from Load_data.data_load import spark
from PysparkUniTestBase import *
from data_transforme.transform_data import TransformData
from data_cleaning.clearData import DataClear
from ShareData import DataSchareToTestClass
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import transform, col, minute
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, FloatType, \
    TimestampType


class PysparkUniTest(PysparkUniTestBase):


    @classmethod
    def tearDownClass(cls):
        """Arrête la session Spark après tous les tests."""
        cls.spark.stop()



    def test_filter_correct_date(self):
        #when
        df = DataSchareToTestClass.createDataFrame(self)

        # given

        result_df = DataClear.filter_correct_date(df, "start_date","2023-1-1","2023-12-31")
        print("testtt",result_df.collect())


        # then
        self.assertTrue(len(result_df.collect())== 1,"test fail")


    def test_correctly_nonzero_passenger_count(self):

        #given
        df = DataSchareToTestClass.createDataFrame(self)

        # when

        result_df = DataClear.filter_nonzero_passenger_count(df,"passenger_count")

        data = [Row(name='Alice', age=25, tp_distance=4.454999923706055, start_date=datetime(2022, 1, 1, 1, 30), end_date=datetime(2022, 1, 1, 2, 30), passenger_count=3)]
        # then
        self.assertEqual(data,result_df.collect(),"error during filter non zero passenger_count")














 # Function to remove rows where `passenger_count` is null or zero




