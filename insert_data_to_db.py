from sqlalchemy import null

from Load_data.data_load import load_data_to_db
from Ressource.file_path_variable import yellow_taxi_schema
from data_transforme.transform_data import  TransformData
from pyspark.sql import SparkSession
from data_cleaning.clearData import delete_passenger_count_eq_null, remove_data_with_zero, remove_row_with_null, \
    remove_all_duplicates, remove_no_macht_date, remove_no_mach_payment
from data_transforme.transform_data import *
from jdbc_variables import *
from pyspark.sql.functions import dayofmonth, col, dayofweek, date_format, years, expr, year, \
    monotonically_increasing_id, isnan, to_timestamp
from taxiSchema.yellowTaxiSchema import *
from pyspark.sql.functions import sum as spark_sum, col





spark = (
          SparkSession
                .builder
                .appName("Yello_test")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.cores", "4")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                .getOrCreate()

)





Taxi_zone_DF = (
    spark
        .read
        .schema(taxi_zone_df_schema)  # Utilisez .schema pour spécifier le schéma
         # Spécifie que le fichier CSV a une en-tête
        .csv("./file_csv/TaxiZones.csv")
)



yellowTaxiDF = (
      spark
        .read
        .parquet("./File_parquet/yellow_tripdata_2023-01.parquet")
)


# inserer une nouvelle colone pour les annee  et les mois les jours et les heures
yellowTaxiDF = (
        yellowTaxiDF
              .withColumn("TripYear",year(col("tpep_pickup_datetime")))
              .withColumn("TripMonth", date_format(col("tpep_pickup_datetime"), "MMMM"))
              .withColumn("TripDays", date_format(col("tpep_pickup_datetime"), "EEEE"))
              .withColumn("TripTime", date_format(col("tpep_pickup_datetime"), "HH:mm:ss"))
         )




#yellowTaxiDF = yellowTaxiDF.repartition("passenger_count")

yellowTaxiDF.createOrReplaceTempView("taxi_yellow")


df=spark.sql("""
          SELECT
          TripDays,
          SUM(trip_distance) AS total_trip_distance, SUM(passenger_count) AS totalPassenger ,(SUM(passenger_count)/SUM(trip_distance)) AS total_passenger_km
          FROM taxi_yellow
          GROUP BY TripDays
          ORDER BY total_passenger_km  DESC
       """)




"""
   data cleaning
"""

df_clean = remove_data_with_zero(yellowTaxiDF,"passenger_count","trip_distance")
df_clean1 = remove_row_with_null(df_clean)
df_clean2 = remove_all_duplicates(df_clean1)

df_clean2 = remove_no_macht_date(df_clean2)

df_clean2.filter((col("tpep_pickup_datetime") <= "2023-01-01") &
            (col("tpep_pickup_datetime") >= "2022-12-30")).show()
df_clean3 = remove_no_macht_date(df_clean2)






"""
    data transform
"""
try:
    renameOneColDF = TransformData.rename_one_column(df_clean2,"payment_type","payment_typeID")
    longToIntegerDf = TransformData.transform_long_to_integer(renameOneColDF,"payment_typeID")
    df_transform = TransformData.transform_float_to_integer(longToIntegerDf,'passenger_count')

    df_rename_col = TransformData.rename_one_column(df_transform ,"store_and_fwd_flag","state")
    df_set_col = TransformData.update_one_column(df_rename_col,"state","N","New_york")
    df_add_col = TransformData.add_column(df_set_col,"trip_distance_in_meter","trip_distance")
    df = TransformData.add_time_for_distance("trip_duration",df_add_col)



    load_data_to_db(df,"yellow_taxi_partition_composite",jdbc_url,db_properties)

    # secondPartitionDf.write \
    #    .jdbc(url=jdbc_url, table="yellow_taxi_partition_2", mode="append", properties=db_properties)

except validation.ColumnNotExistException as e:
    print(e)
except Exception as e:
    print(f"Une erreur inattendue est survenue : {e}")














