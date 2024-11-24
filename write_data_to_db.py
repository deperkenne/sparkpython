from Tools.scripts.generate_opcode_h import header
from numpy import inner
from pandas.core.interchange.dataframe_protocol import DataFrame

from pyspark.sql import SparkSession
from jdbc_variables import *
from pyspark.sql.functions import col


spark = (
          SparkSession
                .builder
                .appName("SparkTablesApp")
                .master("local[*]")
                .config("spark.dynamicAllocation.enables","false")

                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.warehouse.dir", "C:\spark-warehouse")
                #.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") # voici ou la connection a la base de donne s'etabli
                .enableHiveSupport()
                .getOrCreate()

)



# create database

spark.sql("""
    CREATE DATABASE IF NOT EXISTS TaxisDB
""")
# show database
spark.sql("""
    SHOW DATABASES
""").show()
spark.sql(
    """
    SHOW TABLES IN TaxisDB
    """
).show(50)

yellowTaxiDF = (
      spark
        .read
        .parquet("./File_parquet/yellow_tripdata_2023-01.parquet")
)


spark.sql("""
  CREATE TABLE TaxisDB.yellowtaxis (
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        airport_fee DOUBLE
    )
USING PARQUET

LOCATION "./File_parquet/yellow_tripdata_2023-01.parquet/"

"""

)


spark.sql("""
SELECT *
FROM TaxisDB.yellowtaxis
LIMIT 10
""").show()


df = (
      spark
       .read
        .option("header","true")
         .csv("file_csv/Cabs.csv")
    )




df2 = (
      spark
        .read
           .option("mode","DROPMALFORMED")
           .option("columnNameofCorruptRecord","Time")
           .option("multiline","true")
           .json("Json_file/TaxiBases.json")
      )


# modify header en suprimant les espace
newList = [ col.replace(" ","") for col in df2.columns]
df2 = df2.toDF(*newList)



# Exécuter la jointure entre les DataFrames df et df2
df_joined = df.join(df2, df2.LicenseNumber == df.VehicleLicenseNumber, "inner")









df.createOrReplaceTempView("cabs")
df2.createOrReplaceTempView("bases")



"""

# Exemple de données à insérer (tu peux utiliser ton propre DataFrame)
data = [("elephant", "kenne"),
        ("tchofo", "Carved Rock Sports Apparel")]

columns = ["name", "description"]
df = spark.createDataFrame(data, columns)

# Insérer les données dans PostgreSQL
df.write \
    .jdbc(url=jdbc_url, table="taxi", mode="append", properties=db_properties)

"""