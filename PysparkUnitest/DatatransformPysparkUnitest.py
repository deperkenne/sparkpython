from datetime import datetime

from PysparkUnitest.PysparkUniTestBase import PysparkUniTestBase
from data_transforme.transform_data import TransformData
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from parameterized import parameterized
from ShareData import *

class PysparkUniTest(PysparkUniTestBase):


    @classmethod
    def tearDownClass(cls):
        """Arrête la session Spark après tous les tests."""
        cls.spark.stop()

    def test_change_correctly_float_to_int(self):
        # when
        df = DataSchareToTestClass.createDataFrame(self)

        # given SUT
        df = TransformData.transform_float_to_integer(df, "age")
        rows = df.collect()

        # then
        self.assertEqual(True, isinstance(rows[0]["age"], int),
                         f"Le type de 'age' est {type(rows[1]['age'])}, attendu int.")

    def test_add_correctly_new_column(self):
        # when
        df = DataSchareToTestClass.createDataFrame(self)

        # given SUT
        new_df = TransformData.add_column(df, "tpdist_meter", "tp_distance")

        # then
        self.assertNotEqual(df.collect()[0].tpdistance, new_df.collect()[0].tpdist_meter,
                            "conversion km to meter work fail")

    def test_add_correctly_time_column_in_minute(self):
        # given
        df = DataSchareToTestClass.createDataFrame(self)

        # when
        result_df = TransformData.add_time_in_minutes(df, "time_minute", "start_date", "end_date")
        result_df.select("time_minute").show(10)

        # then

        self.assertTrue(len(df.columns) < len(result_df.columns))

    @parameterized.expand([
        # Cas de test (données d'entrée, colonne cible, résultat attendu)
        ("population", 1000000, 300000, 300000),
        ("population", 2000000, 500000, 500000),
    ])
    def test_upDateCorrectlyDf(self, column_name, value, set_value, expected_value):
        # when
        df = DataSchareToTestClass.createDataFrame2(self)

        # given
        new_df = TransformData.update_one_column(df, column_name, value, set_value)
        result_df = new_df.filter(col(column_name) == expected_value)

        # then
        self.assertTrue(result_df.count() > 0, "Test failed: No rows match the expected value")



    def test_add_correctly_new_column_tpdistance(self):
        # when
        df = DataSchareToTestClass.createDataFrame(self)

         # given SUT
        new_df = TransformData.add_column(df,"tp_distance_in_meter","tp_distance")
        # then
        self.assertNotEqual(df.collect()[0].tp_distance, new_df.collect()[0].tp_distance_in_meter, "conversion km to meter work fail")



    def test_rename_one_column_correctly(self):
        # when
        df = DataSchareToTestClass.createDataFrame(self)

        # given SUT
        new_df = TransformData.rename_one_column(df, "name", "firstName")

        # then
        self.assertTrue(new_df.columns[0]!="name","rename column_name fail")



    @parameterized.expand([
        # Cas de test (données d'entrée, colonne cible, résultat attendu)
        ("name", "firstName"),
        ("tp_distance", "tpdistance"),
    ])
    def test_rename_multiple_column_correctly(self,item,val):
        # when
        df = DataSchareToTestClass.createDataFrame(self)

        # given SUT
        new_df = TransformData.rename_multiple_columns(df,**{item: val})

        # then
        self.assertNotIn(item , new_df.columns, "rename column_name fail")


    def test_select_correct_column(self):
        # given
        df = DataSchareToTestClass.createDataFrame(self)

        # when

        result_df = TransformData.select_column(df, "start_date", "end_date")
        expected_list = ["start_date", "end_date"]

        # then

        self.assertTrue(len(result_df.columns) == 2, "select column fail")
        self.assertEqual(result_df.columns, expected_list)


