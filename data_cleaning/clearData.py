
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

defaultValueMap = {'payment_type':5, 'RateCodeID':1}
def delete_passenger_count_eq_null(dataframe):
    df = dataframe.filter(dataframe.passenger_count != 0.0)
    return df

def remove_data_with_zero(df,col1,col2):
     try:
        df =(
            df
            .filter(col(col1) > 0)
            .filter(col(col2) > 0)
        )

     except:
        print("error durring move data equal 0")

     return df






def remove_row_with_null(df):
     try:
        df = (
             df
               .na.drop('all')
        )
     except Exception as ex:
        print("eeror during move null data equal null", ex, sep="  ")
     return df

def fill_null_data_with_default_value(df):
    try:
        df = (
            df
             .na.fill(defaultValueMap)
        )
    except Exception as e :
        print("error during fill data",e, sep="  ")

    return df

def remove_all_duplicates(df):
    try:
        df = (
            df
             .dropDuplicates()   # ceci supprime toute les ligne qui se ressemble si on ne precise pas la colone comme paramettre
        )
    except Exception as e:
         print("error during move duplicate",e, sep="  ")
    return df

def remove_no_mach_payment(df):
    return (
        df
         .filter(df.payment_type > 6)
         .filter(df.payment_type <= 0)
    )


def remove_no_macht_date(df):
  try:
    df = df.filter("tpep_pickup_datetime >= '2023-01-01' AND tpep_dropoff_datetime < '2023-02-01'")
  except Exception as e:
      print("eeror during filter date",e , sep="  ")
  return df