
from flask_appbuilder.security.decorators import limit

from pyspark.sql import SparkSession
from jdbc_variables import *
from pyspark.sql.functions import col, trim, when, regexp_replace

spark = (
          SparkSession
                .builder
                .appName("Yello_test")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                .getOrCreate()

)

df_taxi = spark.read.jdbc(url=jdbc_url, table="taxi", properties=db_properties)
new_row = spark.createDataFrame([(900,"test","newrowtoadd")],["id","name","description"])
df_taxi =df_taxi.union(new_row)

def format_entity_with_space(df):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            # Supprimer les espaces au début de la chaîne
            df = df.withColumn(col_name, when(col(col_name).startswith(" "), regexp_replace(col(col_name), "^\\s+", "")).otherwise(col(col_name)))
    return df

#supprime les espace entre les string
format_entity_with_space(df_taxi).show()
# Infer the schema, and register the DataFrame as a table.
df_taxi.createOrReplaceTempView("taxi") # il permet d'ecrire spark.sql sur le dataframe en attribuent un nom a la table




# SQL can be run over DataFrames that have been registered as a table.
yello_taxi = spark.sql(
    "SELECT name " 
    "FROM taxi"

)


# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
taxi_name = yello_taxi.rdd.map(lambda t: "Name: " + t.name).collect()
for name in taxi_name:
    print(name)
spark.stop()