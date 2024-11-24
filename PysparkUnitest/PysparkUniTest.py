from parameterized import parameterized

from PysparkUniTestBase import *
from data_transforme.transform_data import TransformData
from pyspark import Row


class PysparkUniTest(PysparkUniTestBase):
    @classmethod
    def tearDownClass(cls):
        """Arrête la session Spark après tous les tests."""
        cls.spark.stop()

    def test_change_correctly_float_to_int(self):
        """when"""
        df = self.createDataFrame()

        """ given SUT"""
        df = TransformData.transform_float_to_integer(df, "age")
        rows = df.collect()

        """ then """
        self.assertEqual(True,isinstance(rows[0]["age"], int), f"Le type de 'age' est {type(rows[1]['age'])}, attendu int.")



    def test_add_correctly_new_column(self):
        """when"""
        df = self.createDataFrame()

        """given SUT"""
        new_df = TransformData.add_column(df,"kenne","tpDistance")

        """then"""
        self.assertNotEqual(df.collect()[0].tpdistance, new_df.collect()[0].kenne, "conversion km to meter work fail")
        self.assertTrue(len(df.columns)!=len(new_df.columns),"new column can not add correctly")


    def test_rename_one_column_correctly(self):
        """when"""
        df = self.createDataFrame()

        """given SUT"""
        new_df = TransformData.rename_one_column(df, "name", "firstName")

        """then"""
        self.assertTrue(new_df.columns[0]!="name","rename column_name fail")

    @parameterized.expand([
        # Cas de test (données d'entrée, colonne cible, résultat attendu)
        ("name", "firstName"),
        ("tpdistance", "tp_distance"),
    ])
    def test_rename_multiple_column_correctly(self,item,val):
        """when"""
        df = self.createDataFrame()
        """given SUT"""
        new_df = TransformData.rename_multiple_columns(df,**{item: val})

        """then"""
        self.assertNotIn(item , new_df.columns, "rename column_name fail")

    def createDataFrame(self):
        data = [Row(name="Alice", age="25",tpdistance=4.455), Row(name="Bob", age=30,tpdistance=3.44)]
        df = self.spark.createDataFrame(data)
        return df