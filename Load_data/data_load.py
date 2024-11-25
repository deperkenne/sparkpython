from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = (
          SparkSession
                .builder
                .appName("Yello_test")
                .master("local[*]")
                .config("spark.executor.memory", "1g")
                .getOrCreate()

)


def load_to_csv_file(df,filpath):
    df.write.csv(filpath, header=True)

def load_data_to_db(df,table,jdbcUrl,dbProperties):
    try:
        df.write \
            .jdbc(url=jdbcUrl, table=table, mode="append", properties=dbProperties)
    except Exception as e:
        print(e)