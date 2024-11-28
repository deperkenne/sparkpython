from datetime import datetime

from PysparkUnitest.PysparkUniTestBase import PysparkUniTestBase
from pyspark.sql.types import StructField, StringType, IntegerType, FloatType, TimestampType, StructType, Row


class DataSchareToTestClass(PysparkUniTestBase):
    # Schéma du DataFrame
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("tp_distance", FloatType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True)
    ])

    data =[ ("Alice", 25, 4.455, datetime(2022, 1, 1, 1, 30, 0),
             datetime(2022, 1, 1, 2, 30, 0),3 )]

    @staticmethod
    def createDataFrame(self):
        data = [
            ("Alice", 25, 4.455, datetime(2022, 1, 1, 1, 30, 0),
             datetime(2022, 1, 1, 2, 30, 0), 3

             ),
            ("Bob", 30, 3.44, datetime(2023, 2, 8, 1, 30, 0), datetime(2023, 2, 8, 2, 30, 0),-1)]
        df = self.spark.createDataFrame(data, schema=DataSchareToTestClass.schema)

        return df

    @staticmethod
    def createDataFrame2(self):
        data = [Row(state="newYork", population=1000000), Row(state="Atlanta", population=2000000)]
        df = self.spark.createDataFrame(data)
        return df