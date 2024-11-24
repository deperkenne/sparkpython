

import unittest
from pyspark.sql import SparkSession




def get_spark_session_for_test():
    """
      Crée une session Spark configurée pour les tests.
      Returns:
          SparkSession: Une session Spark locale.
      """
    spark = (
              SparkSession
                    .builder
                    .appName("uni Testing in pyspark ")
                    .master("local[*]")
                    .getOrCreate()
    )
    return spark

class PysparkUniTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialise la session Spark une seule fois pour tous les tests."""
        cls.spark = get_spark_session_for_test()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()





